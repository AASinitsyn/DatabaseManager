// Database Manager - HTMX + Vanilla JS
// State Management
let connections = [];
let selectedConnection = null;
let currentTab = 'query';
let authToken = localStorage.getItem('authToken') || '';

const API_BASE = window.location.origin;

const DB_PORTS = {
    'PostgreSQL': '5432',
    'Elasticsearch': '9200',
    'Meilisearch': '7700',
    'Aerospike': '3000',
    'ClickHouse': '8123',
    'MongoDB': '27017',
    'Cassandra': '9042',
    'Redis': '6379',
    'InfluxDB': '8086',
    'Neo4j': '7474',
    'Couchbase': '8091',
    'Supabase': '5432',
    'Druid': '8888',
    'CockroachDB': '26257',
    'Kafka': '9092',
    'RabbitMQ': '15672',
    'Zookeeper': '2181'
};

const DB_COLORS = {
    'PostgreSQL': 'bg-blue-500',
    'Elasticsearch': 'bg-yellow-500',
    'Meilisearch': 'bg-pink-500',
    'Aerospike': 'bg-red-500',
    'ClickHouse': 'bg-orange-500',
    'MongoDB': 'bg-green-500',
    'Cassandra': 'bg-purple-500',
    'Redis': 'bg-red-600',
    'InfluxDB': 'bg-cyan-500',
    'Neo4j': 'bg-emerald-500',
    'Couchbase': 'bg-indigo-500',
    'Supabase': 'bg-teal-500',
    'Druid': 'bg-amber-500',
    'CockroachDB': 'bg-lime-500',
    'Kafka': 'bg-slate-600',
    'RabbitMQ': 'bg-orange-600',
    'Zookeeper': 'bg-yellow-600'
};

const DATA_TYPES = {
    'PostgreSQL': ['INTEGER', 'BIGINT', 'VARCHAR', 'TEXT', 'BOOLEAN', 'DATE', 'TIMESTAMP', 'JSON', 'UUID'],
    'MongoDB': ['String', 'Number', 'Boolean', 'Date', 'Array', 'Object', 'ObjectId', 'Mixed'],
    'Cassandra': ['int', 'bigint', 'text', 'varchar', 'boolean', 'timestamp', 'uuid', 'list', 'map', 'set'],
    'ClickHouse': ['Int32', 'Int64', 'String', 'Float64', 'Date', 'DateTime', 'UUID', 'Array', 'Nullable'],
    'Elasticsearch': ['text', 'keyword', 'long', 'integer', 'double', 'boolean', 'date', 'object', 'nested'],
    'Meilisearch': ['string', 'number', 'boolean', 'date', 'array', 'object'],
    'Aerospike': ['INTEGER', 'STRING', 'BYTES', 'DOUBLE', 'LIST', 'MAP'],
    'Redis': ['string', 'hash', 'list', 'set', 'zset', 'stream'],
    'InfluxDB': ['float', 'integer', 'string', 'boolean', 'timestamp'],
    'Neo4j': ['String', 'Integer', 'Float', 'Boolean', 'Date', 'DateTime', 'Point', 'Node', 'Relationship'],
    'Couchbase': ['string', 'number', 'boolean', 'array', 'object'],
    'Supabase': ['INTEGER', 'BIGINT', 'VARCHAR', 'TEXT', 'BOOLEAN', 'DATE', 'TIMESTAMP', 'JSON', 'UUID'],
    'Druid': ['STRING', 'LONG', 'DOUBLE', 'FLOAT', 'TIMESTAMP', 'COMPLEX'],
    'CockroachDB': ['INTEGER', 'BIGINT', 'VARCHAR', 'TEXT', 'BOOLEAN', 'DATE', 'TIMESTAMP', 'JSON', 'UUID']
};

const PERMISSIONS = {
    'PostgreSQL': ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'SUPERUSER'],
    'MongoDB': ['read', 'readWrite', 'dbAdmin', 'userAdmin', 'clusterAdmin', 'root'],
    'Cassandra': ['SELECT', 'MODIFY', 'CREATE', 'DROP', 'AUTHORIZE', 'DESCRIBE'],
    'ClickHouse': ['SELECT', 'INSERT', 'ALTER', 'CREATE', 'DROP', 'ADMIN'],
    'Elasticsearch': ['read', 'write', 'manage', 'monitor', 'all'],
    'Meilisearch': ['read', 'write', 'manage'],
    'Aerospike': ['read', 'read-write', 'read-write-udf', 'sys-admin', 'user-admin'],
    'Redis': ['read', 'write', 'admin'],
    'InfluxDB': ['read', 'write', 'admin'],
    'Neo4j': ['reader', 'editor', 'publisher', 'architect', 'admin'],
    'Couchbase': ['read', 'write', 'admin'],
    'Supabase': ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP'],
    'Druid': ['read', 'write', 'admin'],
    'CockroachDB': ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'SUPERUSER']
};

async function apiRequest(url, options = {}) {
    const headers = {
        'Content-Type': 'application/json',
        ...options.headers
    };
    
    if (authToken) {
        headers['Authorization'] = `Bearer ${authToken}`;
    }
    
    const response = await fetch(`${API_BASE}${url}`, {
        ...options,
        headers
    });
    
    if (response.status === 401) {
        localStorage.removeItem('authToken');
        authToken = '';
        window.location.reload();
        return null;
    }
    
    let responseData;
    try {
        const text = await response.text();
        if (!text) {
            responseData = null;
        } else {
            responseData = JSON.parse(text);
        }
    } catch (e) {
        console.error('Ошибка парсинга JSON:', e);
        responseData = {};
    }
    
    if (!response.ok) {
        const errorMsg = responseData?.error || responseData?.warning || `HTTP ${response.status}`;
        const error = new Error(errorMsg);
        error.response = responseData;
        throw error;
    }
    
    if (response.status === 204) {
        return null;
    }
    
    return responseData;
}

function showToast(message, type = 'success') {
    Toastify({
        text: message,
        duration: 3000,
        gravity: "top",
        position: "right",
        backgroundColor: type === 'success' ? '#10b981' : '#ef4444',
    }).showToast();
}

document.addEventListener('DOMContentLoaded', async () => {
    if (!authToken) {
        showLoginModal();
    } else {
        await loadConnections();
        setupFormListeners();
    }
    
    document.getElementById('type')?.addEventListener('change', (e) => {
        // Обновляем порт только если он пустой или если это не режим редактирования
        const portField = document.getElementById('port');
        if (!editingConnectionId && (!portField.value || portField.value === '')) {
            portField.value = DB_PORTS[e.target.value];
        }
    });
});

function showLoginModal() {
    document.body.innerHTML = `
        <div class="min-h-screen bg-gray-50 flex items-center justify-center">
            <div class="bg-white rounded-lg shadow-lg p-8 max-w-md w-full">
                <h2 class="text-2xl font-bold mb-6 text-center">Вход в систему</h2>
                <form id="login-form" class="space-y-4">
                    <div>
                        <label class="block text-sm font-medium mb-2">Имя пользователя</label>
                        <input type="text" id="login-username" required
                            class="w-full px-3 py-2 border rounded-md">
                    </div>
                    <div>
                        <label class="block text-sm font-medium mb-2">Пароль</label>
                        <input type="password" id="login-password" required
                            class="w-full px-3 py-2 border rounded-md">
                    </div>
                    <button type="submit" class="w-full bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700">
                        Войти
                    </button>
                    <button type="button" onclick="showRegisterModal()" class="w-full border px-4 py-2 rounded-md hover:bg-gray-50">
                        Регистрация
                    </button>
                </form>
            </div>
        </div>
    `;
    
    document.getElementById('login-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const username = document.getElementById('login-username').value;
        const password = document.getElementById('login-password').value;
        
        try {
            const result = await apiRequest('/api/auth/login', {
                method: 'POST',
                body: JSON.stringify({ username, password })
            });
            
            if (result && result.token) {
                authToken = result.token;
                localStorage.setItem('authToken', authToken);
                window.location.reload();
            }
        } catch (error) {
            showToast(error.message, 'error');
        }
    });
}

function showRegisterModal() {
    document.body.innerHTML = `
        <div class="min-h-screen bg-gray-50 flex items-center justify-center">
            <div class="bg-white rounded-lg shadow-lg p-8 max-w-md w-full">
                <h2 class="text-2xl font-bold mb-6 text-center">Регистрация</h2>
                <form id="register-form" class="space-y-4">
                    <div>
                        <label class="block text-sm font-medium mb-2">Имя пользователя</label>
                        <input type="text" id="register-username" required
                            class="w-full px-3 py-2 border rounded-md">
                    </div>
                    <div>
                        <label class="block text-sm font-medium mb-2">Пароль</label>
                        <input type="password" id="register-password" required
                            class="w-full px-3 py-2 border rounded-md">
                    </div>
                    <button type="submit" class="w-full bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700">
                        Зарегистрироваться
                    </button>
                    <button type="button" onclick="showLoginModal()" class="w-full border px-4 py-2 rounded-md hover:bg-gray-50">
                        Войти
                    </button>
                </form>
            </div>
        </div>
    `;
    
    document.getElementById('register-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const username = document.getElementById('register-username').value;
        const password = document.getElementById('register-password').value;
        
        try {
            const result = await apiRequest('/api/auth/register', {
                method: 'POST',
                body: JSON.stringify({ username, password })
            });
            
            if (result && result.token) {
                authToken = result.token;
                localStorage.setItem('authToken', authToken);
                window.location.reload();
            }
        } catch (error) {
            showToast(error.message, 'error');
        }
    });
}

async function loadConnections() {
    try {
        const result = await apiRequest('/api/connections');
        if (result === null || result === undefined) {
            connections = [];
        } else if (Array.isArray(result)) {
            connections = result;
        } else {
            console.warn('Неожиданный формат ответа от сервера:', result);
            connections = [];
        }
        renderConnections();
    } catch (error) {
        console.error('Ошибка загрузки подключений:', error);
        showToast('Ошибка загрузки подключений: ' + error.message, 'error');
        connections = [];
        renderConnections();
    }
}

let editingConnectionId = null;

function setupFormListeners() {
    const form = document.getElementById('connection-form');
    if (!form) return;
    
    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const formData = new FormData(e.target);
        const connection = {
            name: formData.get('name') || '',
            type: formData.get('type') || '',
            host: formData.get('host') || '',
            port: formData.get('port') || '',
            database: formData.get('database') || '',
            username: formData.get('username') || '',
            password: formData.get('password') || '',
            ssl: formData.get('ssl') === 'on'
        };
        
        try {
            let result;
            if (editingConnectionId) {
                // Редактирование существующего подключения
                // Пароль всегда отправляем, даже если он не изменился
                result = await apiRequest(`/api/connections/${editingConnectionId}`, {
                    method: 'PUT',
                    body: JSON.stringify(connection)
                });
                
                if (result.warning) {
                    showToast(`Подключение обновлено, но: ${result.warning}`, 'error');
                } else {
                    showToast(`Подключение "${connection.name}" обновлено`);
                }
                editingConnectionId = null;
            } else {
                // Создание нового подключения
                result = await apiRequest('/api/connections', {
                    method: 'POST',
                    body: JSON.stringify(connection)
                });
                
                if (result.warning) {
                    showToast(`Подключение добавлено, но: ${result.warning}`, 'error');
                } else {
                    showToast(`Подключение "${connection.name}" добавлено`);
                }
            }
            
            await loadConnections();
            resetConnectionForm();
        } catch (error) {
            showToast('Ошибка: ' + error.message, 'error');
        }
    });
}

function resetConnectionForm() {
    const form = document.getElementById('connection-form');
    if (form) {
        form.reset();
        editingConnectionId = null;
        const submitBtn = form.querySelector('button[type="submit"]');
        if (submitBtn) {
            submitBtn.textContent = 'Добавить подключение';
        }
        const passwordField = document.getElementById('password');
        if (passwordField) {
            passwordField.required = true;
            passwordField.placeholder = '••••••••';
            passwordField.value = '';
        }
        const passwordHint = document.getElementById('password-hint');
        if (passwordHint) passwordHint.classList.add('hidden');
        const formTitle = document.querySelector('#connection-form').closest('.bg-white').querySelector('h3');
        if (formTitle) {
            formTitle.innerHTML = `
                <svg class="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"/>
                </svg>
                Новое подключение
            `;
        }
    }
}

async function editConnection(id) {
    try {
        // Загружаем полные данные подключения включая пароль
        const conn = await apiRequest(`/api/connections/${id}?edit=true`);
        if (!conn) return;
        
        editingConnectionId = id;
        
        // Заполняем форму данными подключения
        document.getElementById('name').value = conn.name || '';
        document.getElementById('type').value = conn.type || 'PostgreSQL';
        document.getElementById('host').value = conn.host || '';
        document.getElementById('port').value = conn.port || '';
        document.getElementById('database').value = conn.database || '';
        document.getElementById('username').value = conn.username || '';
        // Отображаем пароль в поле
        document.getElementById('password').value = conn.password || '';
        document.getElementById('password').placeholder = '••••••••';
        document.getElementById('password').required = false;
        const passwordHint = document.getElementById('password-hint');
        if (passwordHint) passwordHint.classList.add('hidden');
        document.getElementById('ssl').checked = conn.ssl || false;
        
        // Сохраняем текущий порт, не перезаписываем автоматически при редактировании
        
        // Обновляем заголовок формы
        const formTitle = document.querySelector('#connection-form').closest('.bg-white').querySelector('h3');
        if (formTitle) {
            formTitle.innerHTML = `
                <svg class="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"/>
                </svg>
                Редактирование подключения
            `;
        }
        
        const submitBtn = document.querySelector('#connection-form button[type="submit"]');
        if (submitBtn) {
            submitBtn.textContent = 'Сохранить изменения';
        }
        
        // Прокручиваем к форме
        document.getElementById('connection-form').scrollIntoView({ behavior: 'smooth', block: 'center' });
    } catch (error) {
        showToast('Ошибка загрузки подключения: ' + error.message, 'error');
    }
}

function renderConnections() {
    const container = document.getElementById('connections-list');
    const emptyState = document.getElementById('empty-state');
    
    if (connections.length === 0) {
        container.innerHTML = '';
        emptyState.classList.remove('hidden');
        return;
    }
    
    emptyState.classList.add('hidden');
    container.innerHTML = connections.map(conn => `
        <div class="bg-white rounded-lg border shadow-sm hover:shadow-lg transition-all cursor-pointer ${selectedConnection?.id === conn.id ? 'ring-2 ring-blue-500' : ''}"
             onclick="selectConnection('${conn.id}')">
            <div class="p-4 border-b">
                <div class="flex items-start justify-between mb-2">
                    <div class="flex items-center gap-2">
                        <svg class="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4"/>
                        </svg>
                        <h3 class="font-semibold">${conn.name}</h3>
                    </div>
                    <span class="px-2 py-1 text-xs rounded ${DB_COLORS[conn.type]} text-white">
                        ${conn.type}
                    </span>
                </div>
                <p class="text-sm text-gray-500">${conn.host}:${conn.port}</p>
            </div>
            <div class="p-4">
                <div class="space-y-2">
                    <div class="text-sm">
                        <span class="text-gray-500">База данных:</span>
                        <div class="mt-1 break-words text-gray-900">${conn.database}</div>
                    </div>
                    <div class="flex items-center justify-between text-sm">
                        <span class="text-gray-500">Статус:</span>
                        <span class="px-2 py-1 text-xs rounded ${conn.connected ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-800'}">
                            ${conn.connected ? 'Подключено' : 'Отключено'}
                        </span>
                    </div>
                    <div class="flex gap-2 pt-2">
                        <button onclick="event.stopPropagation(); toggleConnection('${conn.id}')"
                            class="flex-1 px-3 py-1.5 text-sm rounded ${conn.connected ? 'bg-red-600 hover:bg-red-700' : 'bg-blue-600 hover:bg-blue-700'} text-white transition-colors">
                            ${conn.connected ? '⏻ Отключить' : '⏻ Подключить'}
                        </button>
                        <button onclick="event.stopPropagation(); editConnection('${conn.id}')"
                            class="px-3 py-1.5 text-sm border rounded hover:bg-gray-50 transition-colors">
                            ✏️
                        </button>
                        <button onclick="event.stopPropagation(); deleteConnection('${conn.id}')"
                            class="px-3 py-1.5 text-sm border rounded hover:bg-gray-50 transition-colors">
                            🗑️
                        </button>
                    </div>
                </div>
            </div>
        </div>
    `).join('');
}

async function selectConnection(id) {
    selectedConnection = connections.find(c => c.id === id);
    if (selectedConnection && selectedConnection.connected) {
        showWorkspaceView();
    }
    renderConnections();
}

async function toggleConnection(id) {
    const conn = connections.find(c => c.id === id);
    if (!conn) return;
    
    try {
        if (conn.connected) {
            await apiRequest(`/api/connections/${id}/disconnect`, { method: 'POST' });
            await loadConnections();
            showToast('Отключено от базы данных');
        } else {
            const result = await apiRequest(`/api/connections/${id}/connect`, { method: 'POST' });
            await loadConnections();
            if (result && result.error) {
                showToast('Ошибка подключения: ' + result.error, 'error');
            } else {
                showToast('Подключено к базе данных');
            }
        }
    } catch (error) {
        const errorMsg = error.message || 'Неизвестная ошибка';
        showToast('Ошибка: ' + errorMsg, 'error');
        await loadConnections();
    }
}

async function deleteConnection(id) {
    const conn = connections.find(c => c.id === id);
    const connName = conn ? conn.name : 'это подключение';
    
    if (!confirm(`Вы уверены, что хотите удалить подключение "${connName}"?\n\nЭто действие нельзя отменить!`)) {
        return;
    }
    
    try {
        await apiRequest(`/api/connections/${id}`, { method: 'DELETE' });
        if (selectedConnection?.id === id) {
            selectedConnection = null;
            showConnectionsView();
        }
        await loadConnections();
        showToast(`Подключение "${connName}" удалено`);
    } catch (error) {
        showToast('Ошибка удаления: ' + error.message, 'error');
    }
}

function showConnectionsView() {
    document.getElementById('connections-view').classList.remove('hidden');
    document.getElementById('workspace-view').classList.add('hidden');
    document.getElementById('selected-connection-info').innerHTML = '';
}

function showWorkspaceView() {
    document.getElementById('connections-view').classList.add('hidden');
    document.getElementById('workspace-view').classList.remove('hidden');
    
    document.getElementById('selected-connection-info').innerHTML = `
        <div class="flex items-center gap-2">
            <svg class="h-5 w-5 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"/>
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/>
            </svg>
            <div class="text-right">
                <div class="text-sm font-medium">${selectedConnection.name}</div>
                <div class="text-xs text-gray-500">${selectedConnection.type}</div>
            </div>
        </div>
    `;
    
    document.getElementById('workspace-connection-info').innerHTML = `
        <div>
            <h2 class="text-xl font-semibold">${selectedConnection.name}</h2>
            <p class="text-sm text-gray-500">${selectedConnection.type} • ${selectedConnection.host}:${selectedConnection.port}</p>
        </div>
    `;
    
    switchTab('query');
}

function switchTab(tab) {
    currentTab = tab;
    
    if (tab !== 'tables') {
        editingTableName = null;
    }
    
    document.querySelectorAll('.tab-button').forEach(btn => {
        if (btn.dataset.tab === tab) {
            btn.classList.add('border-blue-600', 'text-blue-600');
            btn.classList.remove('border-transparent', 'hover:bg-gray-50');
        } else {
            btn.classList.remove('border-blue-600', 'text-blue-600');
            btn.classList.add('border-transparent', 'hover:bg-gray-50');
        }
    });
    
    const content = document.getElementById('tab-contents');
    
    switch(tab) {
        case 'query':
            content.innerHTML = renderQueryTab();
            setupQueryTab();
            break;
        case 'users':
            content.innerHTML = renderUsersTab();
            setupUsersTab();
            break;
        case 'tables':
            content.innerHTML = renderTablesTab();
            setupTablesTab();
            break;
        case 'databases':
            content.innerHTML = renderDatabasesTab();
            setupDatabasesTab();
            break;
    }
}

function renderQueryTab() {
    return `
        <div class="space-y-4">
            <div class="bg-white rounded-lg border">
                <div class="p-6 border-b">
                    <h3 class="text-lg font-semibold">Редактор запросов</h3>
                    <p class="text-sm text-gray-500">Выполните запрос к базе данных ${selectedConnection.type}</p>
                </div>
                <div class="p-6 space-y-4">
                    <div class="space-y-2">
                        <div class="flex justify-between items-center">
                            <label class="text-sm font-medium">SQL / Query</label>
                            <button onclick="insertExampleQuery()" class="text-sm text-blue-600 hover:underline">
                                Вставить пример
                            </button>
                        </div>
                        <textarea id="query-input" rows="6"
                            class="w-full px-3 py-2 border rounded-md font-mono text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                            placeholder="${getExampleQuery()}"></textarea>
                    </div>
                    <div class="flex gap-2">
                        <button onclick="executeQuery()" class="flex-1 bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700 transition-colors">
                            ▶ Выполнить запрос
                        </button>
                        <button onclick="copyQuery()" class="px-4 py-2 border rounded-md hover:bg-gray-50 transition-colors">
                            📋
                        </button>
                    </div>
                </div>
            </div>
            <div id="query-results"></div>
        </div>
    `;
}

function setupQueryTab() {
}

function getExampleQuery() {
    const examples = {
        'PostgreSQL': 'SELECT * FROM users LIMIT 10;',
        'Elasticsearch': '{"query": {"match_all": {}}}',
        'Meilisearch': '{"q": "", "limit": 20}',
        'Aerospike': 'SELECT * FROM namespace.set LIMIT 10',
        'ClickHouse': 'SELECT * FROM table_name LIMIT 10',
        'MongoDB': '{"name": "test"}',
        'Cassandra': 'SELECT * FROM keyspace.table LIMIT 10;',
        'Redis': 'KEYS *',
        'InfluxDB': 'SELECT * FROM measurement LIMIT 10',
        'Neo4j': 'MATCH (n) RETURN n LIMIT 10',
        'Couchbase': 'SELECT * FROM `bucket` LIMIT 10',
        'Supabase': 'SELECT * FROM users LIMIT 10;',
        'Druid': 'SELECT * FROM datasource LIMIT 10',
        'CockroachDB': 'SELECT * FROM users LIMIT 10;',
        'Kafka': 'Kafka не поддерживает SQL. Используйте Kafka API',
        'RabbitMQ': 'RabbitMQ не поддерживает SQL. Используйте RabbitMQ Management API',
        'Zookeeper': 'Zookeeper не поддерживает SQL. Используйте Zookeeper API'
    };
    return examples[selectedConnection.type] || 'SELECT * FROM table;';
}

function getTableQuery(tableName) {
    const dbType = selectedConnection.type;
    const database = selectedConnection.database || '';
    
    switch(dbType) {
        case 'PostgreSQL':
            return `SELECT * FROM ${tableName} LIMIT 100;`;
        case 'ClickHouse':
            return `SELECT * FROM ${tableName} LIMIT 100`;
        case 'Cassandra':
            const keyspace = database || 'keyspace';
            return `SELECT * FROM ${keyspace}.${tableName} LIMIT 100;`;
        case 'Aerospike':
            const namespace = database || 'namespace';
            return `SELECT * FROM ${namespace}.${tableName} LIMIT 100`;
        case 'MongoDB':
            return `{}`;
        case 'Elasticsearch':
            return `{"query": {"match_all": {}}, "size": 100}`;
        case 'Meilisearch':
            return `{"q": "", "limit": 100}`;
        case 'Redis':
            return `GET ${tableName}`;
        case 'InfluxDB':
            return `SELECT * FROM ${tableName} LIMIT 100`;
        case 'Neo4j':
            return `MATCH (n:${tableName}) RETURN n LIMIT 100`;
        case 'Couchbase':
            return `SELECT * FROM \`${tableName}\` LIMIT 100`;
        case 'Supabase':
            return `SELECT * FROM ${tableName} LIMIT 100;`;
        case 'Druid':
            return `SELECT * FROM ${tableName} LIMIT 100`;
        case 'CockroachDB':
            return `SELECT * FROM ${tableName} LIMIT 100;`;
        default:
            return `SELECT * FROM ${tableName} LIMIT 100;`;
    }
}

function openTableQuery(tableName) {
    switchTab('query');
    
    setTimeout(() => {
        const queryInput = document.getElementById('query-input');
        if (queryInput) {
            queryInput.value = getTableQuery(tableName);
            queryInput.focus();
        }
    }, 100);
}

function insertExampleQuery() {
    document.getElementById('query-input').value = getExampleQuery();
}

function copyQuery() {
    const query = document.getElementById('query-input').value;
    navigator.clipboard.writeText(query);
    showToast('Запрос скопирован в буфер обмена');
}

async function executeQuery() {
    const query = document.getElementById('query-input').value;
    if (!query.trim()) {
        showToast('Введите запрос', 'error');
        return;
    }
    
    try {
        const result = await apiRequest('/api/query', {
            method: 'POST',
            body: JSON.stringify({
                connectionId: selectedConnection.id,
                query: query
            })
        });
        
        if (result.error) {
            showToast('Ошибка выполнения запроса: ' + result.error, 'error');
            displayQueryResults({ columns: [], rows: [], rowCount: 0, executionTime: 0, error: result.error });
        } else {
            displayQueryResults(result);
            showToast(`Запрос выполнен успешно. Найдено строк: ${result.rowCount}`);
        }
    } catch (error) {
        showToast('Ошибка выполнения запроса: ' + error.message, 'error');
    }
}

function displayQueryResults(result) {
    const container = document.getElementById('query-results');
    if (result.error) {
        container.innerHTML = `<div class="bg-red-50 border border-red-200 rounded-lg p-4 text-red-800">${result.error}</div>`;
        return;
    }
    
    container.innerHTML = `
        <div class="bg-white rounded-lg border">
            <div class="p-6 border-b">
                <div class="flex justify-between items-start">
                    <div>
                        <h3 class="text-lg font-semibold">Результаты</h3>
                        <p class="text-sm text-gray-500 mt-1">
                            Строк: ${result.rowCount} • Время: ${result.executionTime}ms
                        </p>
                    </div>
                </div>
            </div>
            <div class="p-6">
                <div class="border rounded-lg overflow-auto max-h-96">
                    <table class="w-full text-sm">
                        <thead class="bg-gray-50">
                            <tr>
                                ${result.columns.map(col => `<th class="px-4 py-2 text-left font-medium">${col}</th>`).join('')}
                            </tr>
                        </thead>
                        <tbody>
                            ${result.rows.map((row, i) => `
                                <tr class="${i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}">
                                    ${result.columns.map(col => `<td class="px-4 py-2 font-mono">${row[col] ?? 'NULL'}</td>`).join('')}
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    `;
}

function renderUsersTab() {
    const permissions = PERMISSIONS[selectedConnection.type] || [];
    
    return `
        <div class="space-y-4">
            <div class="bg-white rounded-lg border">
                <div class="p-6 border-b">
                    <div class="flex items-center justify-between">
                        <div>
                            <h3 class="text-lg font-semibold flex items-center gap-2">
                                <svg class="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z"/>
                                </svg>
                                Пользователи базы данных
                            </h3>
                            <p class="text-sm text-gray-500 mt-1">Управление пользователями ${selectedConnection.type}</p>
                        </div>
                        <button onclick="loadUsers()" class="px-4 py-2 text-sm border rounded-md hover:bg-gray-50 transition-colors">
                            Обновить список
                        </button>
                    </div>
                </div>
                <div class="p-6">
                    <div id="users-list" class="space-y-3">
                        <div class="text-center text-gray-500 py-8">
                            Загрузка пользователей...
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="bg-white rounded-lg border">
                <div class="p-6 border-b">
                    <h3 class="text-lg font-semibold flex items-center gap-2">
                        <svg class="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 9v3m0 0v3m0-3h3m-3 0h-3m-2-5a4 4 0 11-8 0 4 4 0 018 0zM3 20a6 6 0 0112 0v1H3v-1z"/>
                        </svg>
                        Создание пользователя
                    </h3>
                    <p class="text-sm text-gray-500 mt-1">Создайте нового пользователя в базе данных ${selectedConnection.type}</p>
                </div>
                <div class="p-6">
                    <form id="user-form" class="space-y-4">
                        <div class="space-y-2">
                            <label class="block text-sm font-medium">Имя пользователя</label>
                            <input type="text" id="user-username" required
                                class="w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                placeholder="new_user">
                        </div>
                        
                        <div class="grid grid-cols-2 gap-4">
                            <div class="space-y-2">
                                <label class="block text-sm font-medium">Пароль</label>
                                <input type="password" id="user-password" required
                                    class="w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                    placeholder="••••••••">
                            </div>
                            <div class="space-y-2">
                                <label class="block text-sm font-medium">Подтверждение пароля</label>
                                <input type="password" id="user-confirm-password" required
                                    class="w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                    placeholder="••••••••">
                            </div>
                        </div>
                        
                        <div class="space-y-2">
                            <label class="block text-sm font-medium">База данных (опционально)</label>
                            <input type="text" id="user-database"
                                class="w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                placeholder="my_database">
                        </div>
                        
                        <div class="space-y-3">
                            <label class="block text-sm font-medium flex items-center gap-2">
                                Права доступа
                            </label>
                            <div class="grid grid-cols-2 gap-3" id="user-permissions">
                                ${permissions.map(perm => `
                                    <div class="flex items-center gap-2">
                                        <input type="checkbox" id="perm-${perm}" value="${perm}" class="h-4 w-4">
                                        <label for="perm-${perm}" class="text-sm">${perm}</label>
                                    </div>
                                `).join('')}
                            </div>
                        </div>
                        
                        <button type="submit" class="w-full bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700 transition-colors">
                            Создать пользователя
                        </button>
                    </form>
                </div>
            </div>
        </div>
    `;
}

let usersList = [];
let editingUsername = null;

function setupUsersTab() {
    loadUsers();
    
    const form = document.getElementById('user-form');
    if (!form) return;
    
    form.onsubmit = null;
    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        if (editingUsername) {
            return;
        }
        
        const password = document.getElementById('user-password').value;
        const confirmPassword = document.getElementById('user-confirm-password').value;
        
        if (password !== confirmPassword) {
            showToast('Пароли не совпадают', 'error');
            return;
        }
        
        const username = document.getElementById('user-username').value;
        const database = document.getElementById('user-database').value;
        const permissions = Array.from(document.querySelectorAll('#user-permissions input:checked')).map(cb => cb.value);
        
        try {
            await apiRequest('/api/users', {
                method: 'POST',
                body: JSON.stringify({
                    connectionId: selectedConnection.id,
                    username: username,
                    password: password,
                    database: database,
                    permissions: permissions
                })
            });
            
            showToast(`Пользователь ${username} успешно создан`);
            resetUserForm();
            await loadUsers();
        } catch (error) {
            showToast('Ошибка создания пользователя: ' + error.message, 'error');
        }
    });
}

async function loadUsers() {
    const container = document.getElementById('users-list');
    if (!container) return;
    
    try {
        usersList = await apiRequest(`/api/users?connectionId=${selectedConnection.id}`) || [];
        renderUsersList();
    } catch (error) {
        container.innerHTML = `<div class="text-center text-red-500 py-4">Ошибка загрузки пользователей: ${error.message}</div>`;
    }
}

function renderUsersList() {
    const container = document.getElementById('users-list');
    if (!container) return;
    
    if (usersList.length === 0) {
        container.innerHTML = '<div class="text-center text-gray-500 py-8">Пользователи не найдены</div>';
        return;
    }
    
    container.innerHTML = usersList.map(user => `
        <div class="border rounded-lg p-4 hover:bg-gray-50 transition-colors">
            <div class="flex items-center justify-between">
                <div class="flex-1">
                    <div class="flex items-center gap-3">
                        <h4 class="font-semibold">${user.username}</h4>
                        ${user.isSuperuser ? '<span class="px-2 py-1 text-xs rounded bg-purple-100 text-purple-800">SUPERUSER</span>' : ''}
                    </div>
                    ${user.permissions && user.permissions.length > 0 ? `
                        <div class="mt-2 flex flex-wrap gap-2">
                            ${user.permissions.map(perm => `
                                <span class="px-2 py-1 text-xs rounded bg-blue-100 text-blue-800">${perm}</span>
                            `).join('')}
                        </div>
                    ` : '<p class="text-sm text-gray-500 mt-1">Нет прав доступа</p>'}
                </div>
                <div class="flex gap-2">
                    <button onclick="editUser('${user.username}')" 
                        class="px-3 py-1.5 text-sm border rounded hover:bg-gray-50 transition-colors">
                        Редактировать
                    </button>
                    <button onclick="deleteUser('${user.username}')" 
                        class="px-3 py-1.5 text-sm border rounded hover:bg-red-50 text-red-600 transition-colors">
                        Удалить
                    </button>
                </div>
            </div>
        </div>
    `).join('');
}

async function editUser(username) {
    const user = usersList.find(u => u.username === username);
    if (!user) return;
    
    editingUsername = username;
    
    const usernameField = document.getElementById('user-username');
    const passwordField = document.getElementById('user-password');
    const confirmPasswordField = document.getElementById('user-confirm-password');
    
    if (usernameField) usernameField.value = user.username;
    if (usernameField) usernameField.disabled = true;
    if (passwordField) {
        passwordField.required = false;
        passwordField.placeholder = 'Оставьте пустым, чтобы не менять';
    }
    if (confirmPasswordField) {
        confirmPasswordField.required = false;
        confirmPasswordField.placeholder = 'Оставьте пустым, чтобы не менять';
    }
    
    const permissions = user.permissions || [];
    document.querySelectorAll('#user-permissions input').forEach(cb => {
        cb.checked = permissions.includes(cb.value);
    });
    
    const form = document.getElementById('user-form');
    if (!form) return;
    
    const submitBtn = form.querySelector('button[type="submit"]');
    if (submitBtn) submitBtn.textContent = 'Сохранить изменения';
    
    form.scrollIntoView({ behavior: 'smooth', block: 'center' });
    
    const originalHandler = form.onsubmit;
    form.onsubmit = async (e) => {
        e.preventDefault();
        const password = passwordField.value;
        const confirmPassword = confirmPasswordField.value;
        
        if (password && password !== confirmPassword) {
            showToast('Пароли не совпадают', 'error');
            return;
        }
        
        const permissions = Array.from(document.querySelectorAll('#user-permissions input:checked')).map(cb => cb.value);
        
        try {
            await apiRequest('/api/users/update', {
                method: 'PUT',
                body: JSON.stringify({
                    connectionId: selectedConnection.id,
                    username: username,
                    password: password || '',
                    permissions: permissions
                })
            });
            
            showToast(`Пользователь ${username} обновлен`);
            resetUserForm();
            await loadUsers();
        } catch (error) {
            showToast('Ошибка обновления пользователя: ' + error.message, 'error');
        }
    };
}

async function deleteUser(username) {
    if (!confirm(`Вы уверены, что хотите удалить пользователя "${username}"?\n\nЭто действие нельзя отменить!`)) {
        return;
    }
    
    try {
        await apiRequest(`/api/users/delete?connectionId=${selectedConnection.id}&username=${username}`, {
            method: 'DELETE'
        });
        
        showToast(`Пользователь "${username}" удален`);
        await loadUsers();
    } catch (error) {
        showToast('Ошибка удаления пользователя: ' + error.message, 'error');
    }
}

function resetUserForm() {
    const form = document.getElementById('user-form');
    if (!form) return;
    
    form.reset();
    editingUsername = null;
    
    const usernameField = document.getElementById('user-username');
    const passwordField = document.getElementById('user-password');
    const confirmPasswordField = document.getElementById('user-confirm-password');
    
    if (usernameField) usernameField.disabled = false;
    if (passwordField) {
        passwordField.required = true;
        passwordField.placeholder = '••••••••';
    }
    if (confirmPasswordField) {
        confirmPasswordField.required = true;
        confirmPasswordField.placeholder = '••••••••';
    }
    
    const submitBtn = form.querySelector('button[type="submit"]');
    if (submitBtn) submitBtn.textContent = 'Создать пользователя';
    
    form.onsubmit = null;
    
    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        if (editingUsername) {
            return;
        }
        
        const password = document.getElementById('user-password').value;
        const confirmPassword = document.getElementById('user-confirm-password').value;
        
        if (password !== confirmPassword) {
            showToast('Пароли не совпадают', 'error');
            return;
        }
        
        const username = document.getElementById('user-username').value;
        const database = document.getElementById('user-database').value;
        const permissions = Array.from(document.querySelectorAll('#user-permissions input:checked')).map(cb => cb.value);
        
        try {
            await apiRequest('/api/users', {
                method: 'POST',
                body: JSON.stringify({
                    connectionId: selectedConnection.id,
                    username: username,
                    password: password,
                    database: database,
                    permissions: permissions
                })
            });
            
            showToast(`Пользователь ${username} успешно создан`);
            resetUserForm();
            await loadUsers();
        } catch (error) {
            showToast('Ошибка создания пользователя: ' + error.message, 'error');
        }
    }, { once: true });
}

function renderTablesTab() {
    const tableTypeLabel = selectedConnection.type === 'Kafka' ? 'Партиции' :
                          selectedConnection.type === 'RabbitMQ' ? 'Очереди' :
                          selectedConnection.type === 'Zookeeper' ? 'Узлы' :
                          'Таблицы';
    const tableTypeLabelSingle = selectedConnection.type === 'Kafka' ? 'Партиция' :
                                 selectedConnection.type === 'RabbitMQ' ? 'Очередь' :
                                 selectedConnection.type === 'Zookeeper' ? 'Узел' :
                                 'Таблица';
    
    return `
        <div class="space-y-4">
            <div class="bg-white rounded-lg border">
                <div class="p-6 border-b">
                    <div class="flex items-center justify-between">
                        <div>
                            <h3 class="text-lg font-semibold flex items-center gap-2">
                                <svg class="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 10h18M3 14h18m-9-4v8m-7 0h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z"/>
                                </svg>
                                ${tableTypeLabel}
                            </h3>
                            <p class="text-sm text-gray-500 mt-1">Управление ${tableTypeLabel.toLowerCase()} в ${selectedConnection.type}</p>
                        </div>
                        <button onclick="loadTables()" class="px-4 py-2 text-sm border rounded-md hover:bg-gray-50 transition-colors">
                            Обновить список
                        </button>
                    </div>
                </div>
                <div class="p-6">
                    <div id="tables-list" class="space-y-3">
                        <div class="text-center text-gray-500 py-8">
                            Загрузка ${tableTypeLabel.toLowerCase()}...
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="bg-white rounded-lg border">
                <div class="p-6 border-b">
                    <h3 class="text-lg font-semibold flex items-center gap-2">
                        <svg class="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"/>
                        </svg>
                        ${editingTableName ? `Редактирование ${tableTypeLabelSingle.toLowerCase()}` : `Создание ${tableTypeLabelSingle.toLowerCase()}`}
                    </h3>
                    <p class="text-sm text-gray-500 mt-1">${editingTableName ? `Редактируйте ${tableTypeLabelSingle.toLowerCase()}` : `Создайте новую ${tableTypeLabelSingle.toLowerCase()}`} в ${selectedConnection.type}</p>
                </div>
                <div class="p-6">
                    <form id="table-form" class="space-y-4">
                        <div class="space-y-2">
                            <label class="block text-sm font-medium">Название ${tableTypeLabelSingle.toLowerCase()}</label>
                            <input type="text" id="table-name" required
                                class="w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                placeholder="users" ${editingTableName ? 'disabled' : ''}>
                        </div>
                        
                        <div class="space-y-3">
                            <div class="flex justify-between items-center">
                                <label class="block text-sm font-medium">Колонки</label>
                                <button type="button" onclick="addTableColumn()" class="px-3 py-1.5 text-sm border rounded hover:bg-gray-50">
                                    ➕ Добавить колонку
                                </button>
                            </div>
                            <div id="table-columns" class="space-y-3">
                            </div>
                        </div>
                        
                        <button type="submit" class="w-full bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700 transition-colors">
                            ${editingTableName ? 'Сохранить изменения' : 'Создать таблицу'}
                        </button>
                        ${editingTableName ? '<button type="button" onclick="resetTableForm()" class="w-full mt-2 border px-4 py-2 rounded-md hover:bg-gray-50 transition-colors">Отмена</button>' : ''}
                    </form>
                </div>
            </div>
        </div>
    `;
}

let tableColumns = [];
let tablesList = [];
let editingTableName = null;

function setupTablesTab() {
    loadTables();
    
    if (!editingTableName) {
        tableColumns = [];
        addTableColumn();
    }
    
    const form = document.getElementById('table-form');
    if (!form) return;
    
    form.onsubmit = null;
    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const tableName = document.getElementById('table-name').value;
        
        try {
            if (editingTableName) {
                await apiRequest('/api/tables/update', {
                    method: 'PUT',
                    body: JSON.stringify({
                        connectionId: selectedConnection.id,
                        oldName: editingTableName,
                        newName: tableName,
                        columns: tableColumns
                    })
                });
                
                showToast(`Таблица "${tableName}" обновлена`);
            } else {
                await apiRequest('/api/tables', {
                    method: 'POST',
                    body: JSON.stringify({
                        connectionId: selectedConnection.id,
                        name: tableName,
                        columns: tableColumns
                    })
                });
                
                showToast(`Таблица "${tableName}" успешно создана`);
            }
            
            resetTableForm();
            await loadTables();
        } catch (error) {
            showToast('Ошибка: ' + error.message, 'error');
        }
    });
    
    const nameField = document.getElementById('table-name');
    if (nameField) {
        nameField.addEventListener('input', updateTablePreview);
    }
}

async function loadTables() {
    const container = document.getElementById('tables-list');
    if (!container) return;
    
    try {
        tablesList = await apiRequest(`/api/tables?connectionId=${selectedConnection.id}`) || [];
        renderTablesList();
    } catch (error) {
        container.innerHTML = `<div class="text-center text-red-500 py-4">Ошибка загрузки таблиц: ${error.message}</div>`;
    }
}

function renderTablesList() {
    const container = document.getElementById('tables-list');
    if (!container) return;
    
    const tableTypeLabel = selectedConnection.type === 'Kafka' ? 'Партиции' :
                          selectedConnection.type === 'RabbitMQ' ? 'Очереди' :
                          selectedConnection.type === 'Zookeeper' ? 'Узлы' :
                          'Таблицы';
    
    if (tablesList.length === 0) {
        container.innerHTML = `<div class="text-center text-gray-500 py-8">${tableTypeLabel} не найдены</div>`;
        return;
    }
    
    container.innerHTML = tablesList.map(table => `
        <div class="border rounded-lg p-4 hover:bg-gray-50 transition-colors cursor-pointer" onclick="openTableQuery('${table.name}')">
            <div class="flex items-center justify-between">
                <div class="flex-1">
                    <div class="flex items-center gap-2 mb-1">
                        <h4 class="font-semibold">${table.name}</h4>
                        ${table.database ? `<span class="text-xs text-gray-500 bg-gray-100 px-2 py-1 rounded">${table.database}</span>` : (selectedConnection.database ? `<span class="text-xs text-gray-500 bg-gray-100 px-2 py-1 rounded">${selectedConnection.database}</span>` : '')}
                    </div>
                    <div class="mt-2 flex flex-wrap gap-3 text-sm text-gray-600">
                        ${table.size ? `<span><span class="font-medium">Размер:</span> ${table.size}</span>` : ''}
                        ${table.rows !== undefined && table.rows !== null ? `<span><span class="font-medium">Строк:</span> ${table.rows}</span>` : ''}
                    </div>
                </div>
                <div class="flex gap-2 ml-4" onclick="event.stopPropagation()">
                    <button onclick="editTable('${table.name}')" 
                        class="px-3 py-1.5 text-sm border rounded hover:bg-gray-50 transition-colors">
                        Редактировать
                    </button>
                    <button onclick="deleteTable('${table.name}')" 
                        class="px-3 py-1.5 text-sm border rounded hover:bg-red-50 text-red-600 transition-colors">
                        Удалить
                    </button>
                </div>
            </div>
        </div>
    `).join('');
}

async function editTable(name) {
    const table = tablesList.find(t => t.name === name);
    if (!table) return;
    
    editingTableName = name;
    
    const nameField = document.getElementById('table-name');
    if (nameField) {
        nameField.value = table.name;
        nameField.disabled = true;
    }
    
    tableColumns = table.columns || [];
    if (tableColumns.length === 0) {
        addTableColumn();
    } else {
        renderTableColumns();
    }
    
    const form = document.getElementById('table-form');
    if (!form) return;
    
    const formTitle = form.closest('.bg-white').querySelector('h3');
    if (formTitle) {
        formTitle.innerHTML = `
            <svg class="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"/>
            </svg>
            Редактирование таблицы
        `;
    }
    
    const formDesc = form.closest('.bg-white').querySelector('p');
    if (formDesc) {
        formDesc.textContent = `Редактируйте таблицу в базе данных ${selectedConnection.type}`;
    }
    
    const submitBtn = form.querySelector('button[type="submit"]');
    if (submitBtn) submitBtn.textContent = 'Сохранить изменения';
    
    const cancelBtn = form.querySelector('button[type="button"]');
    if (!cancelBtn) {
        const cancelButton = document.createElement('button');
        cancelButton.type = 'button';
        cancelButton.onclick = resetTableForm;
        cancelButton.className = 'w-full mt-2 border px-4 py-2 rounded-md hover:bg-gray-50 transition-colors';
        cancelButton.textContent = 'Отмена';
        submitBtn.after(cancelButton);
    }
    
    form.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

async function deleteTable(name) {
    if (!confirm(`Вы уверены, что хотите удалить таблицу "${name}"?\n\nЭто действие нельзя отменить!`)) {
        return;
    }
    
    try {
        await apiRequest(`/api/tables/delete?connectionId=${selectedConnection.id}&name=${name}`, {
            method: 'DELETE'
        });
        
        showToast(`Таблица "${name}" удалена`);
        await loadTables();
    } catch (error) {
        showToast('Ошибка удаления таблицы: ' + error.message, 'error');
    }
}

function resetTableForm() {
    editingTableName = null;
    tableColumns = [];
    addTableColumn();
    
    const form = document.getElementById('table-form');
    if (!form) return;
    
    form.reset();
    
    const nameField = document.getElementById('table-name');
    if (nameField) {
        nameField.disabled = false;
    }
    
    const formTitle = form.closest('.bg-white').querySelector('h3');
    if (formTitle) {
        formTitle.innerHTML = `
            <svg class="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"/>
            </svg>
            Создание таблицы
        `;
    }
    
    const formDesc = form.closest('.bg-white').querySelector('p');
    if (formDesc) {
        formDesc.textContent = `Создайте новую таблицу в базе данных ${selectedConnection.type}`;
    }
    
    const submitBtn = form.querySelector('button[type="submit"]');
    if (submitBtn) submitBtn.textContent = 'Создать таблицу';
    
    const cancelBtn = form.querySelector('button[type="button"]');
    if (cancelBtn) {
        cancelBtn.remove();
    }
}

function addTableColumn() {
    const columnId = Date.now();
    const types = DATA_TYPES[selectedConnection.type] || ['TEXT'];
    
    tableColumns.push({
        id: columnId,
        name: tableColumns.length === 0 ? 'id' : '',
        type: types[0],
        nullable: tableColumns.length > 0,
        primaryKey: tableColumns.length === 0,
        unique: tableColumns.length === 0
    });
    
    renderTableColumns();
}

function renderTableColumns() {
    const container = document.getElementById('table-columns');
    const types = DATA_TYPES[selectedConnection.type] || ['TEXT'];
    
    container.innerHTML = tableColumns.map((col, index) => `
        <div class="border rounded-lg p-4">
            <div class="grid gap-3">
                <div class="grid grid-cols-2 gap-3">
                    <div class="space-y-2">
                        <label class="block text-sm font-medium">Название</label>
                        <input type="text" value="${col.name}" onchange="updateTableColumn(${index}, 'name', this.value)"
                            class="w-full px-3 py-2 border rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                            placeholder="column_name" required>
                    </div>
                    <div class="space-y-2">
                        <label class="block text-sm font-medium">Тип данных</label>
                        <select onchange="updateTableColumn(${index}, 'type', this.value)"
                            class="w-full px-3 py-2 border rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
                            ${types.map(type => `<option value="${type}" ${col.type === type ? 'selected' : ''}>${type}</option>`).join('')}
                        </select>
                    </div>
                </div>
                
                <div class="flex items-center gap-4">
                    <label class="flex items-center gap-2 text-sm">
                        <input type="checkbox" ${col.nullable ? 'checked' : ''} 
                            onchange="updateTableColumn(${index}, 'nullable', this.checked)" class="h-4 w-4">
                        Nullable
                    </label>
                    <label class="flex items-center gap-2 text-sm">
                        <input type="checkbox" ${col.primaryKey ? 'checked' : ''}
                            onchange="updateTableColumn(${index}, 'primaryKey', this.checked)" class="h-4 w-4">
                        Primary Key
                    </label>
                    <label class="flex items-center gap-2 text-sm">
                        <input type="checkbox" ${col.unique ? 'checked' : ''}
                            onchange="updateTableColumn(${index}, 'unique', this.checked)" class="h-4 w-4">
                        Unique
                    </label>
                    ${tableColumns.length > 1 ? `
                        <button type="button" onclick="removeTableColumn(${index})" 
                            class="ml-auto text-red-600 hover:text-red-700">
                            🗑️
                        </button>
                    ` : ''}
                </div>
            </div>
        </div>
    `).join('');
}

function updateTableColumn(index, field, value) {
    tableColumns[index][field] = value;
}

function removeTableColumn(index) {
    const column = tableColumns[index];
    if (!confirm(`Удалить колонку "${column.name}"?`)) {
        return;
    }
    tableColumns.splice(index, 1);
    renderTableColumns();
}

function updateTablePreview() {
}

function renderDatabasesTab() {
    const dbTypeLabel = selectedConnection.type === 'Cassandra' ? 'Keyspace' :
                        selectedConnection.type === 'Aerospike' ? 'Namespace' :
                        (selectedConnection.type === 'Elasticsearch' || selectedConnection.type === 'Meilisearch') ? 'Индекс' :
                        selectedConnection.type === 'Kafka' ? 'Топик' :
                        selectedConnection.type === 'RabbitMQ' ? 'VHost' :
                        selectedConnection.type === 'Zookeeper' ? 'Узел' :
                        'База данных';
    
    return `
        <div class="space-y-4">
            <div class="bg-white rounded-lg border">
                <div class="p-6 border-b">
                    <div class="flex items-center justify-between">
                        <div>
                            <h3 class="text-lg font-semibold flex items-center gap-2">
                                <svg class="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4"/>
                                </svg>
                                ${dbTypeLabel}ы
                            </h3>
                            <p class="text-sm text-gray-500 mt-1">Список доступных ${dbTypeLabel.toLowerCase()}ов в ${selectedConnection.type}</p>
                        </div>
                        <button onclick="loadDatabases()" class="px-4 py-2 text-sm border rounded-md hover:bg-gray-50 transition-colors">
                            Обновить список
                        </button>
                    </div>
                </div>
                <div class="p-6">
                    <div id="databases-list" class="space-y-3">
                        <div class="text-center text-gray-500 py-8">
                            Загрузка ${dbTypeLabel.toLowerCase()}ов...
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="bg-white rounded-lg border">
                <div class="p-6 border-b">
                    <h3 class="text-lg font-semibold flex items-center gap-2">
                        <svg class="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"/>
                        </svg>
                        Создание ${dbTypeLabel.toLowerCase()}а
                    </h3>
                    <p class="text-sm text-gray-500 mt-1">Создайте новый ${dbTypeLabel.toLowerCase()} в ${selectedConnection.type}</p>
                </div>
                <div class="p-6">
                    <form id="database-form" class="space-y-4">
                        <div class="space-y-2">
                            <label class="block text-sm font-medium">
                                ${selectedConnection.type === 'Cassandra' ? 'Название Keyspace' :
                                  selectedConnection.type === 'Aerospike' ? 'Название Namespace' :
                                  (selectedConnection.type === 'Elasticsearch' || selectedConnection.type === 'Meilisearch') ? 'Название индекса' :
                                  selectedConnection.type === 'Kafka' ? 'Название топика' :
                                  selectedConnection.type === 'RabbitMQ' ? 'Название VHost' :
                                  selectedConnection.type === 'Zookeeper' ? 'Название узла' :
                                  'Название базы данных'}
                            </label>
                            <input type="text" id="db-name" required
                                class="w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                placeholder="my_database">
                        </div>
                        
                        ${selectedConnection.type === 'PostgreSQL' ? `
                            <div class="space-y-2">
                                <label class="block text-sm font-medium">Владелец (Owner)</label>
                                <input type="text" id="db-owner"
                                    class="w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                    placeholder="postgres">
                            </div>
                            <div class="grid grid-cols-2 gap-4">
                                <div class="space-y-2">
                                    <label class="block text-sm font-medium">Кодировка</label>
                                    <select id="db-encoding"
                                        class="w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500">
                                        <option value="UTF8">UTF8</option>
                                        <option value="LATIN1">LATIN1</option>
                                        <option value="SQL_ASCII">SQL_ASCII</option>
                                    </select>
                                </div>
                                <div class="space-y-2">
                                    <label class="block text-sm font-medium">Локаль</label>
                                    <select id="db-locale"
                                        class="w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500">
                                        <option value="en_US.UTF-8">en_US.UTF-8</option>
                                        <option value="ru_RU.UTF-8">ru_RU.UTF-8</option>
                                        <option value="C">C</option>
                                    </select>
                                </div>
                            </div>
                        ` : ''}
                        
                        <button type="submit" class="w-full bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700 transition-colors">
                            Создать ${dbTypeLabel.toLowerCase()}
                        </button>
                    </form>
                </div>
            </div>
        </div>
    `;
}

let databasesList = [];

function setupDatabasesTab() {
    loadDatabases();
    
    const form = document.getElementById('database-form');
    if (!form) return;
    
    form.onsubmit = null;
    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        if (editingDatabaseName) {
            return;
        }
        
        const dbName = document.getElementById('db-name').value;
        const options = {};
        
        if (selectedConnection.type === 'PostgreSQL') {
            const owner = document.getElementById('db-owner')?.value;
            const encoding = document.getElementById('db-encoding')?.value;
            const locale = document.getElementById('db-locale')?.value;
            if (owner) options.owner = owner;
            if (encoding) options.encoding = encoding;
            if (locale) options.locale = locale;
        }
        
        try {
            await apiRequest('/api/databases', {
                method: 'POST',
                body: JSON.stringify({
                    connectionId: selectedConnection.id,
                    name: dbName,
                    options: options
                })
            });
            
            const dbTypeLabel = selectedConnection.type === 'Cassandra' ? 'Keyspace' :
                                selectedConnection.type === 'Aerospike' ? 'Namespace' :
                                (selectedConnection.type === 'Elasticsearch' || selectedConnection.type === 'Meilisearch') ? 'Индекс' :
                                selectedConnection.type === 'Kafka' ? 'Топик' :
                                selectedConnection.type === 'RabbitMQ' ? 'VHost' :
                                selectedConnection.type === 'Zookeeper' ? 'Узел' :
                                'База данных';
            showToast(`${dbTypeLabel} ${dbName} успешно создан`);
            form.reset();
            await loadDatabases();
        } catch (error) {
            showToast('Ошибка создания базы данных: ' + error.message, 'error');
        }
    });
}

async function loadDatabases() {
    const container = document.getElementById('databases-list');
    if (!container) return;
    
    try {
        databasesList = await apiRequest(`/api/databases?connectionId=${selectedConnection.id}`) || [];
        renderDatabasesList();
    } catch (error) {
        container.innerHTML = `<div class="text-center text-red-500 py-4">Ошибка загрузки баз данных: ${error.message}</div>`;
    }
}

let editingDatabaseName = null;

function renderDatabasesList() {
    const container = document.getElementById('databases-list');
    if (!container) return;
    
    if (databasesList.length === 0) {
        const dbTypeLabel = selectedConnection.type === 'Cassandra' ? 'Keyspace' :
                            selectedConnection.type === 'Aerospike' ? 'Namespace' :
                            (selectedConnection.type === 'Elasticsearch' || selectedConnection.type === 'Meilisearch') ? 'Индекс' :
                            selectedConnection.type === 'Kafka' ? 'Топик' :
                            selectedConnection.type === 'RabbitMQ' ? 'VHost' :
                            selectedConnection.type === 'Zookeeper' ? 'Узел' :
                            'База данных';
        container.innerHTML = `<div class="text-center text-gray-500 py-8">${dbTypeLabel}ы не найдены</div>`;
        return;
    }
    
    container.innerHTML = databasesList.map(db => `
        <div class="border rounded-lg p-4 hover:bg-gray-50 transition-colors">
            <div class="flex items-center justify-between">
                <div class="flex-1">
                    <h4 class="font-semibold">${db.name}</h4>
                    <div class="mt-2 flex flex-wrap gap-3 text-sm text-gray-600">
                        ${db.owner ? `<span><span class="font-medium">Владелец:</span> ${db.owner}</span>` : ''}
                        ${db.size ? `<span><span class="font-medium">Размер:</span> ${db.size}</span>` : ''}
                        ${db.encoding ? `<span><span class="font-medium">Кодировка:</span> ${db.encoding}</span>` : ''}
                        ${db.collation ? `<span><span class="font-medium">Collation:</span> ${db.collation}</span>` : ''}
                    </div>
                </div>
                <div class="flex gap-2 ml-4">
                    <button onclick="editDatabase('${db.name}')" 
                        class="px-3 py-1.5 text-sm border rounded hover:bg-gray-50 transition-colors">
                        Редактировать
                    </button>
                    <button onclick="deleteDatabase('${db.name}')" 
                        class="px-3 py-1.5 text-sm border rounded hover:bg-red-50 text-red-600 transition-colors">
                        Удалить
                    </button>
                </div>
            </div>
        </div>
    `).join('');
}

async function editDatabase(name) {
    const db = databasesList.find(d => d.name === name);
    if (!db) return;
    
    editingDatabaseName = name;
    
    const nameField = document.getElementById('db-name');
    if (nameField) {
        nameField.value = db.name;
        nameField.disabled = false;
    }
    
    if (selectedConnection.type === 'PostgreSQL') {
        const ownerField = document.getElementById('db-owner');
        const encodingField = document.getElementById('db-encoding');
        const localeField = document.getElementById('db-locale');
        
        if (ownerField && db.owner) ownerField.value = db.owner;
        if (encodingField && db.encoding) encodingField.value = db.encoding;
        if (localeField && db.collation) {
            const parts = db.collation.split('_');
            if (parts.length >= 2) {
                const locale = parts[0] + '_' + parts[1];
                localeField.value = locale;
            }
        }
    }
    
    const form = document.getElementById('database-form');
    if (!form) return;
    
    const submitBtn = form.querySelector('button[type="submit"]');
    const dbTypeLabel = selectedConnection.type === 'Cassandra' ? 'Keyspace' :
                        selectedConnection.type === 'Aerospike' ? 'Namespace' :
                        (selectedConnection.type === 'Elasticsearch' || selectedConnection.type === 'Meilisearch') ? 'Индекс' :
                        selectedConnection.type === 'Kafka' ? 'Топик' :
                        selectedConnection.type === 'RabbitMQ' ? 'VHost' :
                        selectedConnection.type === 'Zookeeper' ? 'Узел' :
                        'База данных';
    if (submitBtn) submitBtn.textContent = 'Сохранить изменения';
    
    form.scrollIntoView({ behavior: 'smooth', block: 'center' });
    
    form.onsubmit = async (e) => {
        e.preventDefault();
        const newName = nameField.value;
        const options = {};
        
        if (selectedConnection.type === 'PostgreSQL') {
            const owner = document.getElementById('db-owner')?.value;
            if (owner) options.owner = owner;
        }
        
        try {
            await apiRequest('/api/databases/update', {
                method: 'PUT',
                body: JSON.stringify({
                    connectionId: selectedConnection.id,
                    oldName: name,
                    newName: newName,
                    options: options
                })
            });
            
            showToast(`${dbTypeLabel} обновлен`);
            resetDatabaseForm();
            await loadDatabases();
        } catch (error) {
            showToast('Ошибка обновления базы данных: ' + error.message, 'error');
        }
    };
}

async function deleteDatabase(name) {
    const dbTypeLabel = selectedConnection.type === 'Cassandra' ? 'Keyspace' :
                        selectedConnection.type === 'Aerospike' ? 'Namespace' :
                        (selectedConnection.type === 'Elasticsearch' || selectedConnection.type === 'Meilisearch') ? 'Индекс' :
                        selectedConnection.type === 'Kafka' ? 'Топик' :
                        selectedConnection.type === 'RabbitMQ' ? 'VHost' :
                        selectedConnection.type === 'Zookeeper' ? 'Узел' :
                        'База данных';
    
    if (!confirm(`Вы уверены, что хотите удалить ${dbTypeLabel.toLowerCase()} "${name}"?\n\nЭто действие нельзя отменить!`)) {
        return;
    }
    
    try {
        await apiRequest(`/api/databases/delete?connectionId=${selectedConnection.id}&name=${name}`, {
            method: 'DELETE'
        });
        
        showToast(`${dbTypeLabel} "${name}" удален`);
        await loadDatabases();
    } catch (error) {
        showToast('Ошибка удаления базы данных: ' + error.message, 'error');
    }
}

function resetDatabaseForm() {
    const form = document.getElementById('database-form');
    if (!form) return;
    
    form.reset();
    editingDatabaseName = null;
    document.getElementById('db-name').disabled = false;
    const submitBtn = form.querySelector('button[type="submit"]');
    const dbTypeLabel = selectedConnection.type === 'Cassandra' ? 'Keyspace' :
                        selectedConnection.type === 'Aerospike' ? 'Namespace' :
                        (selectedConnection.type === 'Elasticsearch' || selectedConnection.type === 'Meilisearch') ? 'Индекс' :
                        selectedConnection.type === 'Kafka' ? 'Топик' :
                        selectedConnection.type === 'RabbitMQ' ? 'VHost' :
                        selectedConnection.type === 'Zookeeper' ? 'Узел' :
                        'База данных';
    submitBtn.textContent = `Создать ${dbTypeLabel.toLowerCase()}`;
    
    form.onsubmit = null;
    
    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        if (editingDatabaseName) {
            return;
        }
        
        const dbName = document.getElementById('db-name').value;
        const options = {};
        
        if (selectedConnection.type === 'PostgreSQL') {
            const owner = document.getElementById('db-owner')?.value;
            const encoding = document.getElementById('db-encoding')?.value;
            const locale = document.getElementById('db-locale')?.value;
            if (owner) options.owner = owner;
            if (encoding) options.encoding = encoding;
            if (locale) options.locale = locale;
        }
        
        try {
            await apiRequest('/api/databases', {
                method: 'POST',
                body: JSON.stringify({
                    connectionId: selectedConnection.id,
                    name: dbName,
                    options: options
                })
            });
            
            const dbTypeLabel = selectedConnection.type === 'Cassandra' ? 'Keyspace' :
                                selectedConnection.type === 'Aerospike' ? 'Namespace' :
                                (selectedConnection.type === 'Elasticsearch' || selectedConnection.type === 'Meilisearch') ? 'Индекс' :
                                selectedConnection.type === 'Kafka' ? 'Топик' :
                                selectedConnection.type === 'RabbitMQ' ? 'VHost' :
                                selectedConnection.type === 'Zookeeper' ? 'Узел' :
                                'База данных';
            showToast(`${dbTypeLabel} ${dbName} успешно создан`);
            resetDatabaseForm();
            await loadDatabases();
        } catch (error) {
            showToast('Ошибка создания базы данных: ' + error.message, 'error');
        }
    }, { once: true });
}
