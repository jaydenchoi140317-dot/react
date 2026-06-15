/**
 * 청동기 경쟁 (Bronze Age Rivalry) - 게임 서버
 * 
 * 역할:
 *  1. 안티치트   - 클라이언트 해시 + 메모리 값 검증
 *  2. 패드립 필터 - 우회 시도 포함 한국어 욕설 2차 필터링
 *  3. P2P 시그널 - WebSocket 기반 P2P 연결 중계 (게임 연산은 클라이언트)
 *  4. 매칭 시스템 - MMR 기반 팀 매칭 (4인 1팀 고정)
 *  5. 코인/레벨  - 역할별 기여도 기반 보상 계산
 *  6. 인증       - 세션 토큰 발급 및 검증
 */

const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());

// ============================================================
// 상수 / 설정
// ============================================================
const PORT = process.env.PORT || 3000;

/** 허용된 클라이언트 EXE 해시 목록 (업데이트마다 추가) */
const VALID_EXE_HASHES = new Set([
    "5D41402ABC4B2A76B9719D911017C592", // v1.0.0
]);

/** 안티치트 - 서버 기준 허용 범위 */
const ANTICHEAT_LIMITS = {
    stoneDmg:         { max: 35 },   // 뗀석기 칼 기본 데미지
    bronzeThrowDmg:   { max: 50 },   // 비파형동검 투척 데미지
    freezeCooldown:   { min: 45 },   // 얼음 스킬 쿨타임(초)
    stoneKnifeDmg:    { max: 35 },   // 석검 깡뎀 상한
    playerMaxHp:      { exact: 100 },// 플레이어 최대 체력
    inventoryMaxSlot: { max: 11 },   // 삼베자루 최대 슬롯
};

/** MMR 매칭 범위 (대기 시간에 따라 점점 넓어짐) */
const MMR_RANGE_BASE   = 100;  // 초기 MMR 범위
const MMR_RANGE_EXPAND = 50;   // 15초마다 확장
const MMR_RANGE_MAX    = 400;  // 최대 허용 범위

/** 코인 보상 */
const COIN_REWARD = {
    win:              200, // 승리 기본
    lose:             20,  // 패배 기본
    kill:             15,  // 딜러 킬당
    cookingDelivery:  10,  // 요리사 배달당
    craftDelivery:    10,  // 대장장이 제작 지급당
};

// ============================================================
// 인메모리 DB (실제 서비스 시 Redis/PostgreSQL로 교체 권장)
// ============================================================

/** 플레이어 세션: token -> { playerId, username, loginAt } */
const sessions = new Map();

/** 플레이어 데이터: playerId -> { username, mmr, level, coins, wins, losses } */
const players = new Map();

/** 매칭 대기열: [ { playerId, mmr, queuedAt } ] */
let matchQueue = [];

/** 진행중인 방: roomId -> { players: [], hostId, state: 'waiting'|'playing' } */
const rooms = new Map();

/** WebSocket 연결: playerId -> ws */
const wsClients = new Map();

// ============================================================
// 유틸
// ============================================================

function generateToken() {
    return crypto.randomBytes(32).toString('hex');
}

function generateId() {
    return crypto.randomBytes(8).toString('hex');
}

function getPlayer(playerId) {
    return players.get(playerId);
}

function requireSession(req, res) {
    const token = req.headers['x-session-token'];
    if (!token) {
        res.status(401).json({ success: false, message: "세션 토큰 없음" });
        return null;
    }
    const session = sessions.get(token);
    if (!session) {
        res.status(401).json({ success: false, message: "유효하지 않은 세션" });
        return null;
    }
    return session;
}

function broadcast(roomId, data, excludeId = null) {
    const room = rooms.get(roomId);
    if (!room) return;
    const msg = JSON.stringify(data);
    for (const pid of room.players) {
        if (pid === excludeId) continue;
        const ws = wsClients.get(pid);
        if (ws && ws.readyState === 1) ws.send(msg);
    }
}

// ============================================================
// 패드립 필터 (서버 공통 함수)
// ============================================================

/**
 * 우회 탐지 패턴 목록
 * - 한글 자모 단독 표기, 숫자/특수문자 삽입 우회, 초성만 쓰기 등 커버
 */
const PROHIBITED_PATTERNS = [
    // 느금 계열
    /느[\s\W_0-9]*금/i,
    /[ㄴn][\s\W_0-9]*[ㄱg][\s\W_0-9]*[ㅁm]/i,
    // 느개미 계열
    /느[\s\W_0-9]*개[\s\W_0-9]*미/i,
    // 애미/애비 계열
    /애[\s\W_0-9]*미/i,
    /애[\s\W_0-9]*비/i,
    /[ㅇ][\s\W_0-9]*[ㅐ][\s\W_0-9]*[ㅁ]/i,
    /[ㅇ][\s\W_0-9]*[ㅐ][\s\W_0-9]*[ㅂ]/i,
    // 엠창 계열
    /엠[\s\W_0-9]*창/i,
    /[ㅇ][\s\W_0-9]*[ㅔ][\s\W_0-9]*[ㅁ][\s\W_0-9]*[ㅊ]/i,
    // 숫자로 자음 대체 우회 (예: 1=ㅣ+모음, 느1금 등)
    /느\s*[0-9]+\s*금/i,
    /애\s*[0-9]+\s*미/i,
    /애\s*[0-9]+\s*비/i,
    /엠\s*[0-9]+\s*창/i,
];

/**
 * 텍스트를 정규화해서 2차 검사
 * (공백, 특수문자, 숫자 제거 → 한글만 남김)
 */
function normalizeKorean(text) {
    return text.replace(/[^가-힣ㄱ-ㅎㅏ-ㅣa-zA-Z]/g, '');
}

function isPadrip(message) {
    const clean = normalizeKorean(message);
    for (const pattern of PROHIBITED_PATTERNS) {
        if (pattern.test(message) || pattern.test(clean)) {
            return true;
        }
    }
    return false;
}

// ============================================================
// REST API - 인증
// ============================================================

/**
 * POST /api/auth/register
 * 새 플레이어 등록 (처음 실행 시)
 * Body: { username }
 */
app.post('/api/auth/register', (req, res) => {
    const { username } = req.body;
    if (!username || username.length < 2 || username.length > 16) {
        return res.status(400).json({ success: false, message: "닉네임은 2~16자 사이여야 합니다" });
    }
    if (isPadrip(username)) {
        return res.status(400).json({ success: false, message: "사용할 수 없는 닉네임입니다" });
    }

    const playerId = generateId();
    players.set(playerId, {
        playerId,
        username,
        mmr: 1000,
        level: 1,
        xp: 0,
        coins: 0,
        wins: 0,
        losses: 0,
        jobs: [],        // 구매한 직업 목록
        activeJob: null,
    });

    const token = generateToken();
    sessions.set(token, { playerId, username, loginAt: Date.now() });

    res.json({ success: true, playerId, token, message: "등록 완료" });
});

/**
 * POST /api/auth/login
 * 기존 플레이어 로그인 (playerId로 재인증)
 * Body: { playerId }
 */
app.post('/api/auth/login', (req, res) => {
    const { playerId } = req.body;
    const player = players.get(playerId);
    if (!player) {
        return res.status(404).json({ success: false, message: "플레이어를 찾을 수 없습니다" });
    }
    const token = generateToken();
    sessions.set(token, { playerId, username: player.username, loginAt: Date.now() });
    res.json({ success: true, token });
});

// ============================================================
// REST API - 안티치트 (클라이언트 실행 시 호출)
// ============================================================

/**
 * POST /api/auth/verify
 * 클라이언트 무결성 및 메모리 값 검증
 * Body: { clientHash, stoneDmg, bronzeThrowDmg, freezeCooldown, stoneKnifeDmg, playerMaxHp, inventoryMaxSlot }
 */
app.post('/api/auth/verify', (req, res) => {
    const session = requireSession(req, res);
    if (!session) return;

    const {
        clientHash,
        stoneDmg,
        bronzeThrowDmg,
        freezeCooldown,
        stoneKnifeDmg,
        playerMaxHp,
        inventoryMaxSlot,
    } = req.body;

    // 1. EXE 해시 검증
    if (!VALID_EXE_HASHES.has(clientHash)) {
        console.warn(`[안티치트] EXE 변조 감지 - ${session.username} (hash: ${clientHash})`);
        return res.status(403).json({ success: false, message: "클라이언트 변조 감지 - 게임을 다시 설치해주세요" });
    }

    // 2. 메모리 값 검증 (각 항목별로 어떤 값이 이상한지 명시)
    const violations = [];

    if (stoneDmg > ANTICHEAT_LIMITS.stoneDmg.max)
        violations.push(`뗀석기 데미지 변조 (${stoneDmg} > ${ANTICHEAT_LIMITS.stoneDmg.max})`);

    if (bronzeThrowDmg > ANTICHEAT_LIMITS.bronzeThrowDmg.max)
        violations.push(`동검 투척 데미지 변조 (${bronzeThrowDmg} > ${ANTICHEAT_LIMITS.bronzeThrowDmg.max})`);

    if (freezeCooldown < ANTICHEAT_LIMITS.freezeCooldown.min)
        violations.push(`빙결 쿨타임 변조 (${freezeCooldown}s < ${ANTICHEAT_LIMITS.freezeCooldown.min}s)`);

    if (stoneKnifeDmg > ANTICHEAT_LIMITS.stoneKnifeDmg.max)
        violations.push(`석검 데미지 변조 (${stoneKnifeDmg} > ${ANTICHEAT_LIMITS.stoneKnifeDmg.max})`);

    if (playerMaxHp !== ANTICHEAT_LIMITS.playerMaxHp.exact)
        violations.push(`체력 변조 (${playerMaxHp} ≠ ${ANTICHEAT_LIMITS.playerMaxHp.exact})`);

    if (inventoryMaxSlot > ANTICHEAT_LIMITS.inventoryMaxSlot.max)
        violations.push(`인벤토리 슬롯 변조 (${inventoryMaxSlot} > ${ANTICHEAT_LIMITS.inventoryMaxSlot.max})`);

    if (violations.length > 0) {
        console.warn(`[안티치트] 메모리 변조 감지 - ${session.username}:`, violations);
        return res.status(403).json({
            success: false,
            message: "메모리 변조 감지",
            violations,
        });
    }

    // 3. 통과 → P2P 입장 토큰 발급
    const entryToken = generateToken();
    // 토큰에 플레이어ID 매핑 (P2P 연결 시 WebSocket 인증에 사용)
    sessions.set('entry_' + entryToken, { playerId: session.playerId, validUntil: Date.now() + 60000 });

    console.log(`[안티치트] 통과 - ${session.username}`);
    res.json({ success: true, message: "안티치트 검증 통과", entryToken });
});

// ============================================================
// REST API - 채팅 필터
// ============================================================

/**
 * POST /api/chat/send
 * 채팅 메시지 패드립 필터링
 * Body: { message, channel }  channel: 'global' | 'team' | 'pigeon' (비둘기)
 */
app.post('/api/chat/send', (req, res) => {
    const session = requireSession(req, res);
    if (!session) return;

    const { message, channel = 'global' } = req.body;

    if (!message || typeof message !== 'string') {
        return res.status(400).json({ success: false, message: "메시지가 없습니다" });
    }

    // 비둘기 채널: 목판 없으면 10자, 목판 있으면 30자 제한은 클라이언트에서 먼저 차단
    // 서버는 길이만 한 번 더 검증 (클라이언트 우회 방지)
    if (channel === 'pigeon' && message.length > 30) {
        return res.json({ success: true, isFiltered: true, filteredMessage: "", reason: "비둘기가 물어뜯었습니다 (30자 초과)" });
    }

    // 패드립 검사
    if (isPadrip(message)) {
        console.log(`[차단] ${session.username} (${channel}): "${message}"`);
        return res.json({ success: true, isFiltered: true, filteredMessage: "", reason: "패드립 감지" });
    }

    console.log(`[통과] ${session.username} (${channel}): ${message}`);
    res.json({ success: true, isFiltered: false, filteredMessage: message });
});

// ============================================================
// REST API - 매칭
// ============================================================

/**
 * POST /api/match/queue
 * 매칭 대기열 등록
 */
app.post('/api/match/queue', (req, res) => {
    const session = requireSession(req, res);
    if (!session) return;

    const player = getPlayer(session.playerId);
    if (!player) return res.status(404).json({ success: false, message: "플레이어 없음" });

    // 이미 대기 중이면 중복 방지
    const already = matchQueue.find(q => q.playerId === session.playerId);
    if (already) {
        return res.json({ success: true, message: "이미 대기열에 있습니다", queueSize: matchQueue.length });
    }

    matchQueue.push({ playerId: session.playerId, mmr: player.mmr, queuedAt: Date.now() });
    console.log(`[매칭] ${player.username} 대기열 등록 (MMR: ${player.mmr}) | 현재 대기: ${matchQueue.length}명`);

    res.json({ success: true, message: "매칭 대기열 등록됨", queueSize: matchQueue.length });

    // 매칭 시도
    tryMatchmaking();
});

/**
 * DELETE /api/match/queue
 * 대기열 취소
 */
app.delete('/api/match/queue', (req, res) => {
    const session = requireSession(req, res);
    if (!session) return;

    matchQueue = matchQueue.filter(q => q.playerId !== session.playerId);
    res.json({ success: true, message: "대기열 취소됨" });
});

/**
 * MMR 기반 매칭 로직
 * 4인 1팀 → 8명 모이면 방 생성
 * 대기 시간이 길수록 MMR 범위 확장
 */
function tryMatchmaking() {
    if (matchQueue.length < 8) return;

    const now = Date.now();

    // 대기 시간에 따라 각자 허용 MMR 범위 계산
    const candidates = matchQueue.map(q => {
        const waitSec = (now - q.queuedAt) / 1000;
        const expand = Math.floor(waitSec / 15) * MMR_RANGE_EXPAND;
        const range = Math.min(MMR_RANGE_BASE + expand, MMR_RANGE_MAX);
        return { ...q, range };
    });

    // 첫 번째 플레이어 기준으로 MMR 범위 안에 드는 7명 찾기
    for (let i = 0; i < candidates.length; i++) {
        const pivot = candidates[i];
        const group = [pivot];

        for (let j = 0; j < candidates.length && group.length < 8; j++) {
            if (j === i) continue;
            const diff = Math.abs(candidates[j].mmr - pivot.mmr);
            if (diff <= pivot.range) group.push(candidates[j]);
        }

        if (group.length >= 8) {
            const matched = group.slice(0, 8);
            createRoom(matched.map(m => m.playerId));
            // 대기열에서 제거
            const matchedIds = new Set(matched.map(m => m.playerId));
            matchQueue = matchQueue.filter(q => !matchedIds.has(q.playerId));
            return;
        }
    }
}

function createRoom(playerIds) {
    const roomId = generateId();
    rooms.set(roomId, {
        roomId,
        players: playerIds,         // 8명 (팀A: 0-3, 팀B: 4-7)
        hostId: playerIds[0],
        state: 'waiting',
        createdAt: Date.now(),
        gameStartAt: null,
        scores: {},                  // playerId -> { kills, cookings, crafts }
    });

    // 매칭 결과를 WebSocket으로 각 플레이어에게 알림
    const teamA = playerIds.slice(0, 4);
    const teamB = playerIds.slice(4, 8);

    for (const pid of playerIds) {
        const ws = wsClients.get(pid);
        if (ws && ws.readyState === 1) {
            ws.send(JSON.stringify({
                type: 'MATCH_FOUND',
                roomId,
                teamA,
                teamB,
                myTeam: teamA.includes(pid) ? 'A' : 'B',
            }));
        }
    }

    console.log(`[매칭] 방 생성: ${roomId} | 팀A: ${teamA} | 팀B: ${teamB}`);
}

// ============================================================
// REST API - 게임 이벤트 / 코인 정산
// ============================================================

/**
 * POST /api/game/event
 * 클라이언트가 게임 중 이벤트를 서버에 보고 (P2P 검증용)
 * Body: { roomId, event: 'kill'|'cooking'|'craft', targetId? }
 */
app.post('/api/game/event', (req, res) => {
    const session = requireSession(req, res);
    if (!session) return;

    const { roomId, event } = req.body;
    const room = rooms.get(roomId);
    if (!room) return res.status(404).json({ success: false, message: "방 없음" });
    if (!room.players.includes(session.playerId)) {
        return res.status(403).json({ success: false, message: "해당 방의 플레이어가 아님" });
    }

    if (!room.scores[session.playerId]) {
        room.scores[session.playerId] = { kills: 0, cookings: 0, crafts: 0 };
    }
    const s = room.scores[session.playerId];

    if (event === 'kill')     s.kills++;
    else if (event === 'cooking') s.cookings++;
    else if (event === 'craft')   s.crafts++;
    else return res.status(400).json({ success: false, message: "알 수 없는 이벤트" });

    res.json({ success: true });
});

/**
 * POST /api/game/end
 * 게임 종료 처리 - 코인/레벨 정산
 * Body: { roomId, winnerTeam: 'A'|'B' }
 * (호스트 클라이언트 또는 양측 합의로 호출)
 */
app.post('/api/game/end', (req, res) => {
    const session = requireSession(req, res);
    if (!session) return;

    const { roomId, winnerTeam } = req.body;
    const room = rooms.get(roomId);
    if (!room) return res.status(404).json({ success: false, message: "방 없음" });
    if (room.state === 'ended') return res.status(400).json({ success: false, message: "이미 종료된 방" });

    room.state = 'ended';
    const teamA = room.players.slice(0, 4);
    const teamB = room.players.slice(4, 8);

    const results = {};

    for (const pid of room.players) {
        const player = players.get(pid);
        if (!player) continue;

        const isWinner = winnerTeam === 'A' ? teamA.includes(pid) : teamB.includes(pid);
        const score = room.scores[pid] || { kills: 0, cookings: 0, crafts: 0 };

        // 코인 계산
        const baseCoin     = isWinner ? COIN_REWARD.win : COIN_REWARD.lose;
        const killCoin     = score.kills    * COIN_REWARD.kill;
        const cookCoin     = score.cookings * COIN_REWARD.cookingDelivery;
        const craftCoin    = score.crafts   * COIN_REWARD.craftDelivery;
        const totalCoin    = baseCoin + killCoin + cookCoin + craftCoin;

        // MMR 변동 (+20 승 / -15 패)
        const mmrDelta  = isWinner ? 20 : -15;
        player.mmr      = Math.max(0, player.mmr + mmrDelta);

        // 경험치 & 레벨 (100xp per level)
        const xpGained  = isWinner ? 30 : 10;
        player.xp      += xpGained;
        while (player.xp >= player.level * 100) {
            player.xp  -= player.level * 100;
            player.level++;
        }

        // 코인 지급
        player.coins += totalCoin;
        if (isWinner) player.wins++; else player.losses++;

        results[pid] = {
            username: player.username,
            isWinner,
            coinsEarned: totalCoin,
            breakdown: { baseCoin, killCoin, cookCoin, craftCoin },
            mmrDelta,
            newMmr: player.mmr,
            newLevel: player.level,
            totalCoins: player.coins,
        };

        // WebSocket으로 개인 결과 전송
        const ws = wsClients.get(pid);
        if (ws && ws.readyState === 1) {
            ws.send(JSON.stringify({ type: 'GAME_RESULT', ...results[pid] }));
        }
    }

    console.log(`[게임종료] 방 ${roomId} | 승리팀: ${winnerTeam}`);
    res.json({ success: true, results });
});

// ============================================================
// REST API - 상점 (직업 구매)
// ============================================================

/**
 * 구매 가능한 직업 목록
 * 로블록스 데드레일 스타일 - 역할 고정 X, 어느 역할에나 장착 가능
 */
const JOB_CATALOG = {
    swift_runner: {
        id: 'swift_runner', name: '질풍', price: 500,
        description: '이동속도 +20%, 체력 -15',
        stats: { moveSpeed: 1.20, hp: 85, workSpeed: 1.0 },
        startItem: null,
    },
    iron_miner: {
        id: 'iron_miner', name: '광맥꾼', price: 800,
        description: '채굴속도 +40%, 이동속도 -10%',
        stats: { moveSpeed: 0.90, hp: 100, workSpeed: 1.40 },
        startItem: 'SmallBag',  // 시작 시 작은 가죽 자루 지급
    },
    cook_master: {
        id: 'cook_master', name: '솜씨꾼', price: 600,
        description: '요리속도 +35%, 사냥 시 드롭률 +15%',
        stats: { moveSpeed: 1.0, hp: 100, workSpeed: 1.35 },
        startItem: 'DenmunyiPot',  // 덧무늬토기 1개 지급
    },
    berserker: {
        id: 'berserker', name: '광전사', price: 1000,
        description: '공격력 +10%, 체력 +20, 이동속도 -5%',
        stats: { moveSpeed: 0.95, hp: 120, workSpeed: 1.0, atkBonus: 1.10 },
        startItem: 'FlintKnife',  // 뗀석기 칼 지급
    },
    scout: {
        id: 'scout', name: '척후병', price: 700,
        description: '이동속도 +15%, 비둘기 사용 횟수 +1 (판당 4회)',
        stats: { moveSpeed: 1.15, hp: 100, workSpeed: 1.0, pigeonBonus: 1 },
        startItem: null,
    },
};

/**
 * GET /api/shop/jobs
 * 직업 목록 조회
 */
app.get('/api/shop/jobs', (req, res) => {
    const session = requireSession(req, res);
    if (!session) return;
    const player = getPlayer(session.playerId);
    if (!player) return res.status(404).json({ success: false });

    const list = Object.values(JOB_CATALOG).map(job => ({
        ...job,
        owned: player.jobs.includes(job.id),
    }));
    res.json({ success: true, jobs: list, myCoins: player.coins });
});

/**
 * POST /api/shop/buy
 * 직업 구매
 * Body: { jobId }
 */
app.post('/api/shop/buy', (req, res) => {
    const session = requireSession(req, res);
    if (!session) return;
    const player = getPlayer(session.playerId);
    if (!player) return res.status(404).json({ success: false });

    const { jobId } = req.body;
    const job = JOB_CATALOG[jobId];
    if (!job) return res.status(400).json({ success: false, message: "존재하지 않는 직업" });
    if (player.jobs.includes(jobId)) return res.status(400).json({ success: false, message: "이미 보유한 직업" });
    if (player.coins < job.price) return res.status(400).json({ success: false, message: `코인 부족 (보유: ${player.coins}, 필요: ${job.price})` });

    player.coins -= job.price;
    player.jobs.push(jobId);

    res.json({ success: true, message: `${job.name} 직업 구매 완료`, remainCoins: player.coins });
});

/**
 * POST /api/shop/equip
 * 직업 장착
 * Body: { jobId }
 */
app.post('/api/shop/equip', (req, res) => {
    const session = requireSession(req, res);
    if (!session) return;
    const player = getPlayer(session.playerId);
    if (!player) return res.status(404).json({ success: false });

    const { jobId } = req.body;
    if (jobId && !player.jobs.includes(jobId)) {
        return res.status(403).json({ success: false, message: "보유하지 않은 직업" });
    }
    player.activeJob = jobId || null;
    res.json({ success: true, message: jobId ? `${JOB_CATALOG[jobId].name} 장착` : "직업 해제" });
});

// ============================================================
// REST API - 플레이어 정보
// ============================================================

app.get('/api/player/me', (req, res) => {
    const session = requireSession(req, res);
    if (!session) return;
    const player = getPlayer(session.playerId);
    if (!player) return res.status(404).json({ success: false });
    res.json({ success: true, player });
});

// ============================================================
// WebSocket - P2P 시그널 서버
// ============================================================
/*
 * 게임 연산(이동, 전투, 제작)은 전부 클라이언트 P2P로 처리.
 * 이 서버는 P2P 연결 수립에 필요한 SDP/ICE 중계만 담당.
 *
 * 메시지 타입:
 *   CLIENT -> SERVER:
 *     WS_AUTH      { type, entryToken }          → WebSocket 인증
 *     OFFER        { type, roomId, to, sdp }      → WebRTC Offer 전달
 *     ANSWER       { type, roomId, to, sdp }      → WebRTC Answer 전달
 *     ICE          { type, roomId, to, candidate }→ ICE Candidate 전달
 *     ROOM_READY   { type, roomId }               → 게임 준비 완료 신호
 *
 *   SERVER -> CLIENT:
 *     MATCH_FOUND  (매칭 성공 시)
 *     GAME_RESULT  (게임 종료 정산)
 *     RELAY        (상대 플레이어가 보낸 시그널 릴레이)
 *     ERROR        { message }
 */

wss.on('connection', (ws) => {
    let playerId = null;

    ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw); } catch { return; }

        // ── 인증 ──
        if (msg.type === 'WS_AUTH') {
            const entryKey = 'entry_' + msg.entryToken;
            const entry = sessions.get(entryKey);
            if (!entry || entry.validUntil < Date.now()) {
                ws.send(JSON.stringify({ type: 'ERROR', message: 'WebSocket 인증 실패' }));
                ws.close();
                return;
            }
            playerId = entry.playerId;
            wsClients.set(playerId, ws);
            sessions.delete(entryKey); // 일회용 토큰 소비
            ws.send(JSON.stringify({ type: 'WS_AUTH_OK', playerId }));
            console.log(`[WS] 연결됨: ${playerId}`);
            return;
        }

        if (!playerId) {
            ws.send(JSON.stringify({ type: 'ERROR', message: '인증 먼저 해주세요 (WS_AUTH)' }));
            return;
        }

        // ── P2P 시그널 릴레이 ──
        if (['OFFER', 'ANSWER', 'ICE'].includes(msg.type)) {
            const target = wsClients.get(msg.to);
            if (target && target.readyState === 1) {
                target.send(JSON.stringify({ ...msg, from: playerId }));
            }
            return;
        }

        // ── 방 준비 완료 ──
        if (msg.type === 'ROOM_READY') {
            const room = rooms.get(msg.roomId);
            if (!room) return;
            broadcast(msg.roomId, { type: 'PLAYER_READY', playerId }, playerId);
            return;
        }
    });

    ws.on('close', () => {
        if (playerId) {
            wsClients.delete(playerId);
            console.log(`[WS] 연결 끊김: ${playerId}`);
        }
    });
});

// ============================================================
// 헬스체크
// ============================================================
app.get('/', (req, res) => {
    res.json({
        status: 'running',
        game: '청동기 경쟁',
        players: players.size,
        queue: matchQueue.length,
        rooms: rooms.size,
    });
});

// ============================================================
// 서버 시작
// ============================================================
server.listen(PORT, () => {
    console.log(`
╔══════════════════════════════════════════╗
║       청동기 경쟁 - 게임 서버 가동       ║
║  Port: ${PORT}  |  WebSocket: 활성화     ║
╚══════════════════════════════════════════╝
    `);
});
