# Audit Service

## 1. Общие положения

Данный документ описывает предполагаемые REST API эндпоинты сервиса аудита логов чата. API предназначено для:

* сбора и поиска аудиторских событий;
* формирования отчетов;
* предоставления аналитики;
* выявления и обработки подозрительной активности;
* управления политиками хранения и доступом.

Формат обмена данными: JSON
Аутентификация: Bearer Token (JWT / OAuth2)
Все даты и время: UNIX timestamp (ms)

---

## 2. Базовые сущности

### 2.1 AuditEvent

Унифицированная запись аудита.

```json
{
  "event_id": "uuid",
  "event_type": "message.create",
  "timestamp": 1736760000000,
  "user_id": "usr123",
  "actor_user_id": "usr123",
  "topic_id": "grpAbc",
  "status": "success",
  "metadata": {},
  "hash": "sha256:...",
  "prev_hash": "sha256:..."
}
```

**Поля:**

* `event_id` — уникальный идентификатор события
* `event_type` — тип события (login, message.create, topic.delete и т.д.)
* `timestamp` — время события
* `user_id` — пользователь, к которому относится событие
* `actor_user_id` — инициатор действия
* `topic_id` — идентификатор чата (если применимо)
* `status` — success / failure
* `metadata` — специфичные для события данные
* `hash` — хеш записи
* `prev_hash` — хеш предыдущей записи (hash chaining)

---

## 3. Эндпоинты аудиторских событий

### 3.1 Поиск аудиторских событий

**GET** `/api/v1/audit/events`

Используется для поиска и фильтрации аудиторских событий.

**Query parameters:**

* `user_id`
* `actor_user_id`
* `topic_id`
* `event_type`
* `from_ts`
* `to_ts`
* `status`
* `limit`
* `offset`

**Response:**

```json
{
  "total": 1250,
  "events": [ { /* AuditEvent */ } ]
}
```

**Поля:**

* `total` — общее количество найденных событий
* `events` — массив аудиторских записей

---

### 3.2 Получение одного события

**GET** `/api/v1/audit/events/{event_id}`

```json
{
  "event": { /* AuditEvent */ }
}
```

---

## 4. Аудит сообщений

### 4.1 Отчет по сообщениям

**POST** `/api/v1/audit/reports/messages`

Формирует отчет по сообщениям.

**Request:**

```json
{
  "users": ["usr1", "usr2"],
  "topics": ["grp1", "grp2"],
  "from_ts": 1736700000000,
  "to_ts": 1736900000000,
  "include_deleted": true
}
```

**Response:**

```json
{
  "report_id": "uuid",
  "generated_at": 1736901000000,
  "messages": [
    {
      "message_id": 42,
      "topic_id": "grp1",
      "user_id": "usr1",
      "timestamp": 1736800000000,
      "content": "...",
      "hash": "sha256:...",
      "archived_copy_url": "s3://..."
    }
  ]
}
```

**Поля сообщений:**

* `message_id` — seq_id сообщения
* `topic_id` — чат
* `user_id` — автор
* `content` — текст сообщения
* `hash` — хеш содержимого
* `archived_copy_url` — ссылка на архив (если применимо)

---

## 5. Аудит пользователей и сессий

### 5.1 События входа и аутентификации

**GET** `/api/v1/audit/auth-events`

```json
{
  "events": [
    {
      "timestamp": 1736760000000,
      "user_id": "usr1",
      "ip": "192.168.1.10",
      "user_agent": "Chrome",
      "device_id": "dev123",
      "result": "success"
    }
  ]
}
```

**Поля:**

* `ip` — IP-адрес
* `result` — success / failure

---

## 6. Аудит чатов и ролей

### 6.1 События управления чатами

**GET** `/api/v1/audit/topics`

```json
{
  "events": [
    {
      "timestamp": 1736760000000,
      "topic_id": "grp1",
      "action": "create",
      "actor_user_id": "admin"
    }
  ]
}
```

---

### 6.2 События управления участниками

**GET** `/api/v1/audit/subscriptions`

```json
{
  "events": [
    {
      "timestamp": 1736760000000,
      "topic_id": "grp1",
      "user_id": "usr2",
      "action": "role_change",
      "old_role": "read",
      "new_role": "write",
      "actor_user_id": "admin"
    }
  ]
}
```

---

## 7. Файлы (частичный аудит)

### 7.1 Файловые вложения в сообщениях

**GET** `/api/v1/audit/files`

```json
{
  "files": [
    {
      "file_id": "att123",
      "message_id": 42,
      "topic_id": "grp1",
      "user_id": "usr1",
      "name": "doc.pdf",
      "size": 102400,
      "mime_type": "application/pdf",
      "hash": "sha256:..."
    }
  ]
}
```

---

## 8. Аналитика

### 8.1 Агрегированные показатели

**GET** `/api/v1/analytics/summary`

```json
{
  "from_ts": 1736700000000,
  "to_ts": 1736900000000,
  "events_by_type": {
    "message.create": 1200,
    "login.success": 340
  },
  "top_users": [
    { "user_id": "usr1", "count": 320 }
  ],
  "by_hour": [
    { "hour": 10, "count": 120 }
  ]
}
```

---

### 8.2 Drill-down аналитика

**GET** `/api/v1/analytics/timeseries`

```json
{
  "metric": "message.create",
  "interval": "hour",
  "points": [
    { "ts": 1736760000000, "value": 32 }
  ]
}
```

---

## 9. Подозрительная активность

### 9.1 Список инцидентов

**GET** `/api/v1/incidents`

```json
{
  "incidents": [
    {
      "incident_id": "inc123",
      "type": "anomaly.login",
      "status": "open",
      "detected_at": 1736760000000
    }
  ]
}
```

---

### 9.2 Детализация инцидента

**GET** `/api/v1/incidents/{incident_id}`

```json
{
  "incident_id": "inc123",
  "events": [ { /* AuditEvent */ } ],
  "user_profile": {
    "user_id": "usr1",
    "typical_activity": {}
  }
}
```

---

### 9.3 Обновление статуса инцидента

**POST** `/api/v1/incidents/{incident_id}/status`

```json
{
  "status": "confirmed",
  "comment": "Подтверждено администратором"
}
```

---

## 10. Политики хранения

### 10.1 Получение retention-политик

**GET** `/api/v1/retention`

```json
{
  "policies": [
    {
      "event_type": "message.*",
      "retention_days": 365
    }
  ]
}
```

---

### 10.2 Обновление retention-политики

**POST** `/api/v1/retention`

```json
{
  "event_type": "login.*",
  "retention_days": 180
}
```

---

## 11. Экспорт данных

### 11.1 Экспорт аудита

**POST** `/api/v1/audit/export`

```json
{
  "filters": {
    "user_id": "usr1",
    "from_ts": 1736700000000,
    "to_ts": 1736900000000
  },
  "format": "csv"
}
```

**Response:**

```json
{
  "export_id": "exp123",
  "download_url": "https://..."
}
```

---

## 12. Роли и доступ

(Описываются на уровне auth middleware; эндпоинты возвращают 403 при отсутствии прав)

---

## 13. Версионирование API

Все эндпоинты версионируются через `/api/v1/`.

---

**Конец документа**
