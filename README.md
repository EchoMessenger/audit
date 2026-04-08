# Audit Service

## 1. Общие положения

Данный документ описывает REST API эндпоинты сервиса аудита логов чата. API предназначено для:

* поиска и фильтрации аудиторских событий;
* формирования отчетов по сообщениям;
* предоставления аналитики (summary, timeseries);
* выявления и обработки инцидентов безопасности;
* экспорта данных (CSV, JSON);
* просмотра политик хранения данных (TTL).

**Технологии:**
- Spring Boot 3 + Kotlin
- ClickHouse (аналитическая БД)
- JWT Bearer Token аутентификация
- Cursor-based пагинация

**Формат обмена данными:** JSON  
**Аутентификация:** Bearer Token (JWT)  
**Даты и время:** UNIX timestamp (миллисекунды)

---

## 2. Базовые сущности

### 2.1 AuditEvent

Унифицированная запись аудита. Объединяет данные из разных ClickHouse таблиц (`client_req_log`, `message_log`, `topic_log`, `subscription_log`, `account_log`).

```json
{
  "eventId": "uuid-or-log-id",
  "eventType": "message.create",
  "timestamp": 1736760000000,
  "userId": "usr123",
  "actorUserId": "usr123",
  "topicId": "grpAbc",
  "status": "success",
  "metadata": {},
  "ip": "192.168.1.10",
  "userAgent": "Chrome/120",
  "deviceId": "dev123"
}
```

**Поля:**

* `eventId` — уникальный идентификатор события
* `eventType` — тип события (см. маппинг ниже)
* `timestamp` — время события в миллисекундах UNIX
* `userId` — пользователь, к которому относится событие
* `actorUserId` — инициатор действия (может отличаться от userId)
* `topicId` — идентификатор чата (если применимо)
* `status` — `success` | `failure`
* `metadata` — специфичные для события данные (JSON object)
* `ip`, `userAgent`, `deviceId` — опциональные поля для auth-событий

**Маппинг типов событий:**

| Источник | msg_type/action | eventType |
|----------|----------------|-----------|
| client_req_log | LOGIN | auth.login |
| client_req_log | HI | auth.session_start |
| client_req_log | BYE | auth.logout |
| client_req_log | REG | auth.register |
| message_log | PUB | message.create |
| message_log | EDIT | message.edit |
| message_log | DEL | message.delete |
| topic_log | CREATE | topic.create |
| topic_log | DELETE | topic.delete |
| account_log | UPDATE | account.update |
| subscription_log | JOIN | subscription.join |
| subscription_log | LEAVE | subscription.leave |
| subscription_log | ROLE | subscription.role_change |

### 2.2 Cursor Pagination

Все list-эндпоинты используют cursor-based пагинацию для стабильности при вставке новых данных.

**Cursor format:** base64-encoded string `timestamp_ms:log_id`

```json
{
  "data": [ /* AuditEvent[] */ ],
  "nextCursor": "encoded-cursor-string",
  "hasMore": true
}
```

---

## 3. Эндпоинты аудиторских событий

### 3.1 Поиск аудиторских событий

**GET** `/api/v1/audit/events`

Универсальный поиск событий из всех аудиторских таблиц с фильтрацией.

**Query parameters:**

* `userId` (optional) — фильтр по пользователю
* `actorUserId` (optional) — фильтр по инициатору
* `topicId` (optional) — фильтр по топику/чату
* `eventType` (optional) — фильтр по типу события
* `fromTs` (optional) — начало временного диапазона (UNIX ms)
* `toTs` (optional) — конец временного диапазона (UNIX ms)
* `status` (optional) — фильтр по статусу (`success` | `failure`)
* `cursor` (optional) — cursor для пагинации
* `limit` (optional, default: 100, max: 1000) — количество записей

**Response:**

```json
{
  "data": [
    {
      "eventId": "evt-123",
      "eventType": "message.create",
      "timestamp": 1736760000000,
      "userId": "usr1",
      "actorUserId": "usr1",
      "topicId": "grp1",
      "status": "success",
      "metadata": {}
    }
  ],
  "nextCursor": "MTczNjc2MDAwMDAwMDpldnQtMTIz",
  "hasMore": false
}
```

**HTTP Status:**
- 200 OK
- 400 Bad Request (если `toTs <= fromTs`)
- 401 Unauthorized (без токена)
- 403 Forbidden (нет роли `audit_read` или `audit_admin`)

**Required Roles:** `audit_read` или `audit_admin`

---

### 3.2 Получение одного события

**GET** `/api/v1/audit/events/{eventId}`

Получение детальной информации о конкретном событии.

**Path parameters:**
* `eventId` — идентификатор события

**Response:**

```json
{
  "event": {
    "eventId": "evt-123",
    "eventType": "message.create",
    "timestamp": 1736760000000,
    "userId": "usr1",
    "topicId": "grp1",
    "status": "success",
    "metadata": {
      "messageContent": "Hello"
    }
  }
}
```

**HTTP Status:**
- 200 OK
- 404 Not Found

**Required Roles:** `audit_read` или `audit_admin`

---

### 3.3 События аутентификации

**GET** `/api/v1/audit/auth-events`

Специализированный эндпоинт для событий аутентификации (LOGIN, HI, BYE, REG).

**Query parameters:**

* `userId` (optional)
* `fromTs` (optional)
* `toTs` (optional)
* `cursor` (optional)
* `limit` (optional, default: 100, max: 1000)

**Response:**

```json
{
  "data": [
    {
      "eventId": "evt-456",
      "eventType": "auth.login",
      "timestamp": 1736760000000,
      "userId": "usr1",
      "status": "success",
      "ip": "192.168.1.10",
      "userAgent": "Chrome/120",
      "deviceId": "dev123",
      "metadata": {}
    }
  ],
  "nextCursor": null,
  "hasMore": false
}
```

**Required Roles:** `audit_read` или `audit_admin`

---

### 3.4 Сессии пользователей

**GET** `/api/v1/audit/sessions`

Аналитика сессий пользователей, сгруппированная по `sess_session_id`.

**Query parameters:**

* `userId` (optional)
* `fromTs` (optional)
* `toTs` (optional)
* `cursor` (optional)
* `limit` (optional, default: 50, max: 500)

**Response:**

```json
{
  "data": [
    {
      "sessionId": "sess-789",
      "userId": "usr1",
      "firstEventAt": 1736760000000,
      "lastEventAt": 1736763600000,
      "durationSeconds": 3600,
      "eventCount": 42,
      "ipAddresses": ["192.168.1.10"]
    }
  ],
  "nextCursor": null,
  "hasMore": false
}
```

**Required Roles:** `audit_read` или `audit_admin`

---

### 3.5 Хронология пользователя (Timeline)

**GET** `/api/v1/audit/users/{userId}/timeline`

Полная хронологическая история пользователя: объединяет client_req_log, message_log и subscription_log.

**Path parameters:**
* `userId` — идентификатор пользователя

**Query parameters:**

* `fromTs` (optional)
* `toTs` (optional)
* `cursor` (optional)
* `limit` (optional, default: 100, max: 500)

**Response:**

```json
{
  "data": [
    {
      "eventId": "evt-1",
      "eventType": "auth.login",
      "timestamp": 1736760000000,
      "userId": "usr1",
      "status": "success"
    },
    {
      "eventId": "evt-2",
      "eventType": "message.create",
      "timestamp": 1736760100000,
      "userId": "usr1",
      "topicId": "grp1",
      "status": "success"
    }
  ],
  "nextCursor": null,
  "hasMore": false
}
```

**Required Roles:** `audit_read` или `audit_admin`

---

## 4. Аудит сообщений

### 4.1 Отчет по сообщениям

**POST** `/api/v1/audit/reports/messages`

Формирует отчет по сообщениям за указанный период. 

**Логика работы:**
- Для периодов ≤ 7 дней — синхронный ответ (200 OK)
- Для периодов > 7 дней — асинхронный экспорт (202 Accepted)

**Rate Limit:** 5 запросов/минуту на пользователя

**Request:**

```json
{
  "users": ["usr1", "usr2"],
  "topics": ["grp1", "grp2"],
  "fromTs": 1736700000000,
  "toTs": 1736900000000,
  "includeDeleted": true
}
```

**Response (синхронный, период ≤ 7 дней):**

```json
{
  "reportId": "rpt-uuid",
  "generatedAt": 1736901000000,
  "totalMessages": 150,
  "messages": [
    {
      "messageId": 42,
      "topicId": "grp1",
      "userId": "usr1",
      "userName": "John Doe",
      "timestamp": 1736800000000,
      "content": "Hello world",
      "isDeleted": false
    }
  ]
}
```

**Response (асинхронный, период > 7 дней):**

```json
{
  "message": "Period exceeds 7 days — export started asynchronously",
  "exportId": "exp-uuid",
  "status": "pending",
  "createdAt": 1736900000000,
  "pollUrl": "/api/v1/audit/export/exp-uuid"
}
```

**HTTP Status:**
- 200 OK (синхронный отчет)
- 202 Accepted (асинхронный экспорт)
- 429 Too Many Requests (превышен rate limit)

**Headers (при 429):**
- `X-Rate-Limit-Retry-After-Seconds: 60`

**Required Roles:** `audit_read` или `audit_admin`

---

## 5. Аналитика

### 5.1 Агрегированные показатели

**GET** `/api/v1/analytics/summary`

Агрегированные показатели за указанный период из materialized views ClickHouse.

**Query parameters:**

* `fromTs` (required) — начало периода
* `toTs` (required) — конец периода

**Response:**

```json
{
  "eventsByType": {
    "message.create": 1200,
    "auth.login": 340,
    "topic.create": 15
  },
  "topUsers": [
    {
      "userId": "usr1",
      "eventCount": 320
    }
  ],
  "byHour": [
    {
      "hour": 10,
      "eventCount": 120
    }
  ]
}
```

**HTTP Status:**
- 200 OK
- 400 Bad Request (если `fromTs` или `toTs` отсутствуют, или `toTs <= fromTs`)

**Required Roles:** `audit_read` или `audit_admin`

---

### 5.2 Временные ряды (Timeseries)

**GET** `/api/v1/analytics/timeseries`

Временной ряд по конкретной метрике с заданным интервалом.

**Query parameters:**

* `metric` (required) — метрика: `message.create`, `auth.login`, `auth.register`, `topic.create`
* `interval` (optional, default: `hour`) — интервал группировки: `hour` | `day`
* `fromTs` (required)
* `toTs` (required)

**Response:**

```json
{
  "metric": "message.create",
  "interval": "hour",
  "from_ts": 1736700000000,
  "to_ts": 1736900000000,
  "points": [
    {
      "ts": 1736760000000,
      "value": 32
    },
    {
      "ts": 1736763600000,
      "value": 45
    }
  ]
}
```

**Required Roles:** `audit_read` или `audit_admin`

---

## 6. Инциденты безопасности

### 6.1 Список инцидентов

**GET** `/api/v1/incidents`

Получение списка инцидентов безопасности с фильтрацией.

**Query parameters:**

* `status` (optional) — фильтр по статусу: `open`, `confirmed`, `dismissed`
* `type` (optional) — фильтр по типу: `brute_force`, `concurrent_sessions`, `mass_delete`, `volume_anomaly`, `topic_enumeration`, `inactive_account_activation`, `off_hours_activity`, `privilege_escalation`
* `userId` (optional) — фильтр по пользователю
* `limit` (optional, default: 100, max: 1000)

**Response:**

Note: This endpoint does not use cursor-based pagination (exception to section 2.2). Returns a simple array response.

```json
{
  "incidents": [
    {
      "incidentId": "inc-123",
      "type": "brute_force",
      "status": "open",
      "detectedAt": 1736760000000,
      "userId": "usr1",
      "details": {
        "failed_login_attempts": 12,
        "window_minutes": 5
      },
      "updatedAt": 1736760000000
    }
  ]
}
```

**Required Roles:** `audit_read` или `audit_admin`

---

### 6.2 Детализация инцидента

**GET** `/api/v1/incidents/{incidentId}`

Получение детальной информации об инциденте, включая связанные события (±5 минут вокруг времени обнаружения).

**Path parameters:**
* `incidentId` — идентификатор инцидента

**Response:**

```json
{
  "incident": {
    "incidentId": "inc-123",
    "type": "brute_force",
    "status": "open",
    "detectedAt": 1736760000000,
    "userId": "usr1",
    "details": {},
    "updatedAt": 1736760000000
  },
  "events": [
    {
      "eventId": "evt-1",
      "eventType": "auth.login",
      "timestamp": 1736759900000,
      "userId": "usr1",
      "status": "failure",
      "ip": "1.2.3.4"
    }
  ]
}
```

**HTTP Status:**
- 200 OK
- 404 Not Found

**Required Roles:** `audit_read` или `audit_admin`

---

### 6.3 Обновление статуса инцидента

**POST** `/api/v1/incidents/{incidentId}/status`

Обновление статуса инцидента администратором.

**Path parameters:**
* `incidentId` — идентификатор инцидента

**Request:**

```json
{
  "status": "confirmed",
  "comment": "Подтверждено администратором"
}
```

**Allowed status values:**
- `open`
- `confirmed`
- `dismissed`

**Response:**

```json
{
  "incidentId": "inc-123",
  "type": "brute_force",
  "status": "confirmed",
  "detectedAt": 1736760000000,
  "userId": "usr1",
  "details": {},
  "updatedAt": 1736770000000
}
```

**HTTP Status:**
- 200 OK
- 400 Bad Request (невалидный статус)
- 404 Not Found (инцидент не найден)

**Required Roles:** `audit_admin` (только администраторы могут обновлять статус)

---

## 7. Экспорт данных

### 7.1 Запуск экспорта

**POST** `/api/v1/audit/export`

Асинхронный экспорт аудиторских данных в CSV или JSON.

**Rate Limit:** 2 запроса/минуту на пользователя

**Request:**

```json
{
  "filters": {
    "userId": "usr1",
    "topicId": "grp1",
    "users": ["usr1", "usr2"],
    "topics": ["grp1", "grp2"],
    "eventType": "message.create",
    "fromTs": 1736700000000,
    "toTs": 1736900000000,
    "status": "success",
    "includeDeleted": false
  },
  "format": "csv"
}
```

**Fields:**
- `filters.users` — если задан, имеет приоритет над `filters.userId`
- `filters.topics` — если задан, имеет приоритет над `filters.topicId`
- `format` — `csv` | `json`

**Response:**

```json
{
  "export_id": "exp-123",
  "status": "pending",
  "format": "csv",
  "created_at": 1736900000000
}
```

**HTTP Status:**
- 202 Accepted
- 429 Too Many Requests

**Required Roles:** `audit_read` или `audit_admin`

---

### 7.2 Статус экспорта

**GET** `/api/v1/audit/export/{exportId}`

Получение статуса и информации о задаче экспорта.

**Path parameters:**
* `exportId` — идентификатор экспорта

**Response:**

```json
{
  "exportId": "exp-123",
  "status": "completed",
  "format": "csv",
  "createdAt": 1736900000000,
  "completedAt": 1736900060000,
  "downloadUrl": "/api/v1/audit/export/exp-123/download",
  "errorMessage": null,
  "fileSizeBytes": 1024000
}
```

**Status values:**
- `pending` — ожидает обработки
- `running` — в процессе экспорта
- `completed` — завершен успешно
- `failed` — ошибка при экспорте

**HTTP Status:**
- 200 OK
- 404 Not Found

**Required Roles:** `audit_read` или `audit_admin`

---

### 7.3 Скачивание экспорта

**GET** `/api/v1/audit/export/{exportId}/download`

Скачивание файла экспорта.

**Behavior:**
- Для PVC storage — прямая отдача файла (200 OK)
- Для S3 storage — редирект на presigned URL (302 Found)

**Path parameters:**
* `exportId` — идентификатор экспорта

**Response (PVC):**
- Content-Type: `text/csv` или `application/json`
- Content-Disposition: `attachment; filename="audit-export-{exportId}.{format}"`

**Response (S3):**
- HTTP 302 Found
- Location: presigned S3 URL

**HTTP Status:**
- 200 OK (PVC, файл готов)
- 202 Accepted (экспорт еще не завершен)
- 302 Found (S3 redirect)
- 404 Not Found (файл не найден)

**Required Roles:** `audit_read` или `audit_admin`

---

## 8. Политики хранения

### 8.1 Получение retention-политик

**GET** `/api/v1/retention`

Чтение реальных TTL значений из ClickHouse `system.tables`.

**Response:**

```json
{
  "policies": [
    {
      "tableName": "client_req_log",
      "database": "audit",
      "engine": "MergeTree",
      "ttlExpression": "timestamp + INTERVAL 365 DAY",
      "retentionDays": 365
    },
    {
      "tableName": "message_log",
      "database": "audit",
      "engine": "MergeTree",
      "ttlExpression": "timestamp + INTERVAL 730 DAY",
      "retentionDays": 730
    }
  ]
}
```

**Note:** Обновление TTL выполняется через Helm values и миграции БД, не через API.

**Required Roles:** `audit_read` или `audit_admin`

---

## 9. Аутентификация и авторизация

### Роли

API использует Spring Security с JWT токенами. Доступны две роли:

* **`audit_read`** — чтение всех эндпоинтов (GET)
* **`audit_admin`** — чтение + изменение статусов инцидентов (POST)

### HTTP Headers

```
Authorization: Bearer <JWT_TOKEN>
```

### Коды ответов

| Код | Описание |
|-----|----------|
| 200 | OK — успешный запрос |
| 202 | Accepted — задача принята в обработку (async) |
| 400 | Bad Request — невалидные параметры |
| 401 | Unauthorized — отсутствует или невалидный токен |
| 403 | Forbidden — недостаточно прав (требуется audit_admin) |
| 404 | Not Found — ресурс не найден |
| 429 | Too Many Requests — превышен rate limit |

---

## 10. Rate Limiting

Некоторые эндпоинты имеют ограничения частоты запросов:

| Эндпоинт | Лимит |
|----------|-------|
| POST /api/v1/audit/reports/messages | 5 запросов/мин |
| POST /api/v1/audit/export | 2 запроса/мин |

При превышении лимита:
- HTTP 429 Too Many Requests
- Header: `X-Rate-Limit-Retry-After-Seconds: 60`

---

## 11. Версионирование API

Все эндпоинты версионируются через `/api/v1/`.

---

## 12. Технические детали

### База данных

**ClickHouse** — колоночная СУБД для аналитики.

**Таблицы:**
- `client_req_log` — клиентские запросы (LOGIN, HI, BYE, REG)
- `message_log` — сообщения (PUB, EDIT, DEL)
- `topic_log` — операции с чатами (CREATE, DELETE)
- `subscription_log` — подписки (JOIN, LEAVE, ROLE)
- `account_log` — изменения аккаунтов (UPDATE)
- `incident_log` — инциденты безопасности

### Пагинация

Cursor-based пагинация обеспечивает стабильность при concurrent inserts.

**Cursor format:** `base64(timestamp_ms:log_id)`

### Storage для экспортов

Поддерживается два типа storage:
- **PVC** (Persistent Volume Claim) — файлы на диске pod
- **S3** — объектное хранилище (presigned URLs для скачивания)

---

## 13. Примеры использования

### Пример 1: Найти все логины пользователя за последний час

```bash
curl -X GET "http://localhost:8080/api/v1/audit/auth-events?userId=usr123&fromTs=1736756400000&toTs=1736760000000" \
  -H "Authorization: Bearer <token>"
```

### Пример 2: Получить аналитику за день

```bash
curl -X GET "http://localhost:8080/api/v1/analytics/summary?fromTs=1736726400000&toTs=1736812800000" \
  -H "Authorization: Bearer <token>"
```

### Пример 3: Создать экспорт сообщений

```bash
curl -X POST "http://localhost:8080/api/v1/audit/export" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "filters": {
      "topicId": "grp1",
      "fromTs": 1736700000000,
      "toTs": 1736800000000
    },
    "format": "csv"
  }'
```

### Пример 4: Обновить статус инцидента

```bash
curl -X POST "http://localhost:8080/api/v1/incidents/inc-123/status" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "dismissed",
    "comment": "False positive"
  }'
```

---

## 14. Тестирование

Для тестирования API доступна Postman коллекция:
- Расположение: `/tests/audit-service-postman-collection.json`
- Документация: `/tests/README.md`

---

**Конец документа**
