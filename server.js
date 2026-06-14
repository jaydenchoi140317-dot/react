const express = require('express');
const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const VALID_EXE_HASH = "5D41402ABC4B2A76B9719D911017C592";

// 우회 패드립을 정교하게 잡기 위한 정규식(Regex) 데이터베이스
const PROHIBITED_PATTERNS = [
    /느[\s0-9A-Za-z~!@#$%^&*()_+=\-\[\]{}|;':",./<>?]*금/i,      
    /느[\s0-9A-Za-z~!@#$%^&*()_+=\-\[\]{}|;':",./<>?]*개[\s0-9A-Za-z~!@#$%^&*()_+=\-\[\]{}|;':",./<>?]*미/i, 
    /애[\s0-9A-Za-z~!@#$%^&*()_+=\-\[\]{}|;':",./<>?]*미/i,      
    /애[\s0-9A-Za-z~!@#$%^&*()_+=\-\[\]{}|;':",./<>?]*비/i,      
    /엠[\s0-9A-Za-z~!@#$%^&*()_+=\-\[\]{}|;':",./<>?]*창/i,      
    /[ㄴn][\s0-9A-Za-z~!@#$%^&*()_+=\-\[\]{}|;':",./<>?]*[ㄱg][\s0-9A-Za-z~!@#$%^&*()_+=\-\[\]{}|;':",./<>?]*[ㅁm]/i 
];

// 안티치트 문지기 검증 API
app.post('/api/auth/verify', (req, res) => {
    const { clientHash, stoneDmg, bronzeThrowDmg, freezeCooldown } = req.body;
    if (clientHash !== VALID_EXE_HASH) {
        return res.status(403).json({ success: false, message: "클라이언트 변조 감지" });
    }
    if (stoneDmg > 35 || bronzeThrowDmg > 50 || freezeCooldown < 45) {
        return res.status(403).json({ success: false, message: "메모리 변조 감지" });
    }
    res.json({ success: true, message: "안티치트 검증 통과", entryToken: "P2P_ACCESS_TOKEN_2026" });
});

// 지능형 패드립 필터링 채팅 API
app.post('/api/chat/send', (req, res) => {
    const { username, message } = req.body;
    
    // 공백 및 특수문자 제거 후 2차 검사
    const cleanText = message.replace(/[\s0-9~!@#$%^&*()_+=\-\[\]{}|;':",./<>?]/g, "");

    let isPadripDetected = false;

    for (const pattern of PROHIBITED_PATTERNS) {
        if (pattern.test(message) || pattern.test(cleanText)) {
            isPadripDetected = true;
            break;
        }
    }

    if (isPadripDetected) {
        console.log(`[차단] ${username}: "${message}"`);
        return res.json({ success: true, isFiltered: true, filteredMessage: "" });
    }

    console.log(`[통과] ${username}: ${message}`);
    res.json({ success: true, isFiltered: false, filteredMessage: message });
});

app.get('/', (req, res) => {
    res.send("청동기 경쟁 Node.js 백엔드가 정상 작동 중입니다.");
});

app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
