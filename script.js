const authScreen = document.getElementById("auth-screen");
const chatScreen = document.getElementById("chat-screen");
const authTabs = document.querySelectorAll(".auth-tab");
const authUsername = document.getElementById("auth-username");
const authPassword = document.getElementById("auth-password");
const authError = document.getElementById("auth-error");
const authSubmit = document.getElementById("auth-submit");

let authMode = "login"; // "login" или "register"

authTabs.forEach((tab) => {
    tab.addEventListener("click", () => {
        authMode = tab.dataset.mode;
        authTabs.forEach((t) => t.classList.toggle("active", t === tab));
        authSubmit.textContent = authMode === "login" ? "Войти" : "Зарегистрироваться";
        authError.style.display = "none";
    });
});

authSubmit.addEventListener("click", async () => {
    const username = authUsername.value;
    const password = authPassword.value;
    if (!username || !password) {
        return;
    }
    try {
        const response = await fetch("/" + authMode, {
            method: "POST",
            credentials: "same-origin",
            body: JSON.stringify({ username, password }),
        });
        if (!response.ok) {
            const text = await response.text();
            authError.textContent = text || "Ошибка";
            authError.style.display = "block";
            return;
        }
        startChat(username);
    } catch (err) {
        authError.textContent = "Не удалось связаться с сервером";
        authError.style.display = "block";
    }
});

function startChat(myNickname) {
    authScreen.classList.remove("visible");
    chatScreen.style.display = "flex";

    const roomList = document.getElementById("room-list");
    const roomCreateInput = document.getElementById("room-create-input");
    const messages = document.getElementById("messages");
    const input = document.getElementById("input");
    const replyPreview = document.getElementById("reply-preview");
    const replyPreviewText = document.getElementById("reply-preview-text");
    const replyPreviewCancel = document.getElementById("reply-preview-cancel");

    let socket = null;
    let currentRoom = null;
    let messagesById = new Map(); // для резолва цитаты по id, только среди уже отрисованных в текущей комнате
    let replyingTo = null; // { id, nickname, text } или null
    let drafts = new Map(); // недописанный текст в input, отдельно на каждую комнату

    function setReplyingTo(msg) {
        replyingTo = msg;
        if (msg) {
            replyPreviewText.textContent = "Ответ " + msg.nickname + ": " + msg.text;
            replyPreview.classList.add("active");
        } else {
            replyPreview.classList.remove("active");
        }
    }

    replyPreviewCancel.addEventListener("click", () => setReplyingTo(null));

    function renderMessage(data) {
        messagesById.set(data.id, { nickname: data.nickname, text: data.text });

        const line = document.createElement("div");
        line.classList.add("message", data.nickname === myNickname ? "own" : "other");

        if (data.reply_to) {
            const quoted = messagesById.get(data.reply_to);
            const quoteDiv = document.createElement("span");
            quoteDiv.classList.add("quote");
            quoteDiv.textContent = quoted ? quoted.nickname + ": " + quoted.text : "сообщение";
            line.appendChild(quoteDiv);
        }

        const nameSpan = document.createElement("span");
        nameSpan.classList.add("nickname");
        nameSpan.textContent = data.nickname;

        const textSpan = document.createElement("span");
        textSpan.classList.add("text");
        textSpan.textContent = ": " + data.text;

        const replyBtn = document.createElement("span");
        replyBtn.classList.add("reply-btn");
        replyBtn.textContent = "↩";
        replyBtn.addEventListener("click", () => setReplyingTo({ id: data.id, nickname: data.nickname, text: data.text }));

        line.appendChild(nameSpan);
        line.appendChild(textSpan);
        line.appendChild(replyBtn);
        messages.appendChild(line);
    }

    function connectToRoom(room) {
        if (room === currentRoom) {
            return;
        }
        if (socket) {
            socket.onclose = null; // не реагируем на закрытие, которое сами же и инициировали
            socket.onmessage = null; // старое соединение не должно рисовать в уже переключённый чат
            socket.close();
        }
        if (currentRoom !== null) {
            drafts.set(currentRoom, input.value);
        }
        currentRoom = room;
        messages.innerHTML = "";
        messagesById = new Map();
        setReplyingTo(null);
        highlightActiveRoom();
        input.value = drafts.get(room) || "";

        socket = new WebSocket("ws://" + location.host + "/ws?room=" + encodeURIComponent(room));
        socket.onmessage = (event) => renderMessage(JSON.parse(event.data));
        input.focus();
    }

    function highlightActiveRoom() {
        roomList.querySelectorAll(".room-item").forEach((el) => {
            el.classList.toggle("active", el.dataset.room === currentRoom);
        });
    }

    function renderRooms(roomsMap) {
        const names = Object.keys(roomsMap).sort();
        roomList.innerHTML = "";
        names.forEach((name) => {
            const item = document.createElement("div");
            item.classList.add("room-item");
            item.dataset.room = name;
            item.textContent = name;
            const count = document.createElement("span");
            count.classList.add("room-count");
            count.textContent = (roomsMap[name] || []).length;
            item.appendChild(count);
            item.addEventListener("click", () => connectToRoom(name));
            roomList.appendChild(item);
        });
        highlightActiveRoom();
    }

    async function refreshRooms() {
        try {
            const response = await fetch("/rooms", { credentials: "same-origin" });
            if (!response.ok) {
                return;
            }
            const roomsMap = await response.json();
            renderRooms(roomsMap);
        } catch (err) {
            // тихо игнорируем — следующий тик обновит
        }
    }

    roomCreateInput.addEventListener("keydown", async (event) => {
        if (event.key === "Enter" && roomCreateInput.value !== "") {
            const name = roomCreateInput.value;
            roomCreateInput.value = "";
            try {
                const response = await fetch("/rooms", {
                    method: "POST",
                    credentials: "same-origin",
                    body: JSON.stringify({ name }),
                });
                if (!response.ok) {
                    return;
                }
            } catch (err) {
                return;
            }
            connectToRoom(name);
            refreshRooms(); // не ждать следующего тика опроса, обновить сайдбар сразу
        }
    });

    input.addEventListener("keydown", (event) => {
        if (event.key === "Enter" && input.value !== "" && socket) {
            const payload = { text: input.value };
            if (replyingTo) {
                payload.reply_to = replyingTo.id;
            }
            socket.send(JSON.stringify(payload));
            input.value = "";
            setReplyingTo(null);
        }
    });

    messages.textContent = "Выбери комнату слева или создай новую";
    refreshRooms();
    setInterval(refreshRooms, 1500);
}

// при загрузке страницы — проверить, нет ли уже валидной сессии (cookie),
// чтобы не заставлять логиниться заново после обновления страницы.
// Пока идёт проверка, оба экрана скрыты (см. #auth-screen в CSS) —
// чтобы не мелькал экран входа перед тем, как окажется, что мы уже залогинены.
(async () => {
    try {
        const response = await fetch("/me", { credentials: "same-origin" });
        if (response.ok) {
            const data = await response.json();
            startChat(data.username);
            return;
        }
    } catch (err) {
        // сервер недоступен — тоже считаем, что не залогинены
    }
    authScreen.classList.add("visible");
})();
