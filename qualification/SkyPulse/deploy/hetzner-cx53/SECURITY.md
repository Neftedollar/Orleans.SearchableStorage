# Безопасность и приватность развёртывания

Этот набор предназначен для первой калибровки SkyPulse на двух CX53: один процесс
приложения/Orleans Memory index/dispatcher рядом с TAP и отдельный PostgreSQL-хост. Это не
высокодоступная схема, не разрешение публиковать пользовательские данные и не готовый
qualification-вердикт. Одновременно разрешён ровно один экземпляр приложения и один dispatcher.

## Что запрещено класть в репозиторий

Никогда не коммитить и не включать в Docker build context:

- пароли, токены, connection strings и настоящие `.env`-файлы;
- `routing.private.manifest.json`, `routing.private.ndjson`, DID-журналы и acquisition workspace;
- PostgreSQL/TAP data directories, дампы, WAL, резервные копии и журналы;
- настоящий `accounts.ak32` и другие артефакты конкретного запуска без отдельного privacy-review.

В репозитории допустимы только шаблоны с явно недействительными значениями.
`Dockerfile.app.dockerignore` построен по белому списку, а Dockerfile копирует только
solution/config, `src`, `tests`, проверенный пакет и entrypoint. Нельзя заменять это на широкий
`COPY . .` или обычный deny-list: private artifacts не должны даже отправляться build daemon.

## Пользователи, секреты и права

Запускать Web и TAP от разных непривилегированных системных пользователей. Не выдавать им
`sudo`, Linux capabilities или доступ к Docker socket. Для unit/container включить
`NoNewPrivileges`, запрет core dump (`LimitCORE=0`) и read-only root filesystem, оставив
запись только в явно названные data directories.

Использовать разные случайные секреты минимум по 32 байта:

- пароль роли PostgreSQL приложения;
- пароль отдельной роли/БД TAP;
- общий для TAP и Web `TAP_ADMIN_PASSWORD` / `SkyPulse__Durable__TapAdminPassword`;
- `SkyPulse__Durable__CorpusGrowthAdminToken`;
- пароль закрытого UI и ключ шифрования backup.

Секреты не передавать в аргументах команд или URL. Родительский каталог секретов принадлежит
root и имеет режим `0700`; отдельные файлы приложения устанавливаются владельцу UID 10001,
файлы TAP — UID 65534, файлы PostgreSQL — UID `postgres` закреплённого образа. Режим файла —
`0400` (допустим `0600`). Compose монтирует их read-only, а entrypoints читают значения до
запуска процесса; реальные значения не сохраняются в compose YAML или `appsettings*.json`.

Для каждого разрешённого corpus profile закрытый маршрут хранится отдельно:

```text
/var/lib/skypulse/private-routes/<profile>/
  routing.private.manifest.json
  routing.private.ndjson
```

Каталог обязан быть обычным, не symlink, строго `0700`; оба файла — обычные, не symlink,
строго `0600`. Владельцем должен быть пользователь Web-процесса. Монтировать весь каталог в Web
только для чтения: отдельные Compose secrets обычно получают `0444` и этому контракту не
соответствуют. Родительские каталоги также не должны быть доступны недоверенным пользователям.

`SourceInstanceId` не является паролем, но это постоянная идентичность конкретной TAP DB. Его
нужно сохранить вместе с конфигурацией и нельзя менять при обычном перезапуске или обновлении.

## Сеть и TLS

Рекомендуемая матрица входящих соединений. Hetzner Cloud Firewall не фильтрует
private Cloud Network, поэтому checked guest nftables обязателен на обоих хостах;
Docker и SSH имеют systemd `Requires=` на соответствующий firewall service:

| Хост | Порт | Кто может подключаться |
| --- | ---: | --- |
| app/TAP | 22 | только административные IP или VPN |
| app/TAP | 80/443 | только явно заданные `public_ui_cidrs`; закрыты при пустом списке |
| app/TAP | 5080 | только `127.0.0.1`, Caddy и локальный health check |
| app/TAP | 2480 | только `127.0.0.1`, Web-процесс |
| PostgreSQL | 5432 | только app/TAP `10.42.0.10`; PostgreSQL находится на `10.42.0.20` |
| PostgreSQL | 22 | только административные IP или VPN |

Не публиковать Orleans silo/gateway, TAP admin API, PostgreSQL, metrics или pprof. TAP должен
иметь `TAP_BIND=127.0.0.1:2480`; Web —
`ASPNETCORE_URLS=http://127.0.0.1:5080`. Код разрешает `ws://` только для loopback. При разных
network namespace требуется внутренний `wss://` с проверяемым сертификатом, а не обход проверки.

C закрытыми `public_ui_cidrs` работать через SSH/VPN tunnel прямо к loopback Web. Если UI
открывается на 80/443, Caddy завершает публичный TLS и проксирует в Kestrel по loopback. Включить
HSTS после проверки домена, CSP (`default-src 'self'`, `object-src 'none'`,
`frame-ancestors 'none'`),
`X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer` и ограничение request body.

По умолчанию весь UI закрывается VPN или Basic Auth. Query API не имеет прикладной авторизации,
а UI показывает детерминированные account keys. Если UI сознательно открывают публично, Caddy
должен блокировать `/api/corpus-capacity*` и `/health`; рост выполняется только через SSH/VPN.
POST роста всё равно обязан содержать `X-SkyPulse-Corpus-Admin`. Для публичного query API нужен
отдельно утверждённый rate limit: встроенного ограничения запросов сейчас нет.

## PostgreSQL

Приложение и TAP используют разные базы и роли без `SUPERUSER`, `CREATEDB` и взаимного доступа.
Роль приложения может владеть только своей БД/схемой: при старте Web сам применяет и проверяет
схему. В `pg_hba.conf` разрешать только точные private IP, БД и роли с `hostssl` и
`scram-sha-256`.

Connection string приложения должен явно использовать `SSL Mode=VerifyFull`, локальный CA и имя,
совпадающее с SAN сертификата PostgreSQL; проверка в текущем коде сама этого не требует. Для TAP
действует тот же принцип. Диск PostgreSQL, private routes и off-host backups должны быть
зашифрованы.

OpenTofu в этом bundle создаёт обычные root-диски и сам не настраивает LUKS/зашифрованный volume.
Это жёсткий внешний prerequisite: до размещения реальных private routes, PGDATA, секретов или
backup локальный агент обязан реализовать и restore-test'нуть encryption-at-rest либо представить
отдельно проверенную гарантию провайдера для точных дисков и backup. Без неё разрешена только
пустая/синтетическая калибровка.

## Журналы и публичные свидетельства

Не включать Caddy access log по умолчанию: он может сохранить URI, IP и заголовки. Никогда не
логировать `Authorization` или `X-SkyPulse-Corpus-Admin`. Для ASP.NET установить
`Microsoft.AspNetCore=Warning`; внешние APM/error collectors по умолчанию выключить.

TAP очищает тела и произвольные ошибки до записи, но его допустимые operational logs всё ещё могут
содержать DID, rkey, CID и revision. Хранить их в закрытом локальном sink с конечным сроком
(не более 7 дней и с ограничением размера), без публичного экспорта. Compose пишет в journald,
а checked drop-in задаёт persistent storage, `MaxRetentionSec=7day`, `SystemMaxUse=2G` и
`SystemKeepFree=5G`; оба preflight сверяют его байт-в-байт и требуют active service. Выключить core dumps и swap либо
использовать только зашифрованный swap. Публичные отчёты содержат агрегаты и SHA-256 артефактов,
но не DID, record paths или детерминированные DID/account hashes.

## Резервные копии и срок хранения

PostgreSQL — авторитетное состояние; Memory index не копируется и после старта полностью
перестраивается из PostgreSQL. TAP DB, приложение и его `SourceInstanceId` образуют одну цепочку
доставки. `backup-postgres.sh` создаёт только один компонент recovery set: физическую копию
кластера с обеими БД на совместимую точку. Полный закрытый recovery set дополнительно обязан
содержать соответствующие DB secret files (для восстановленных SCRAM verifier), оба release
record, конфигурацию профиля, TLS material, public corpus manifests и зашифрованные private
routes.

В bundle включён ручной согласованный `pg_basebackup` всего кластера перед обновлением/ростом.
Он не является непрерывным PITR: `archive_mode` в начальной калибровке намеренно выключен. До
длительного production-потока оператор обязан отдельно спроектировать и проверить непрерывный
WAL archive, off-host шифрование, сбор всех перечисленных компонентов и конечную политику
хранения. Bundle намеренно не выбирает storage/encryption vendor и не считает один каталог
`pg_basebackup` полным recovery set. Первый restore drill выполняется
до реального потока, затем перед увеличением corpus cap. Восстановление проверяет TAP marker v3, точное
число repositories, `SourceInstanceId`, права `0700/0600`, `mode=Durable`, готовность `/health` и
полную перестройку индекса.

`PostgreSqlRetentionStore` реализован, но сейчас не подключён к Web и не имеет maintenance CLI.
Не заменять его прямым cron `DELETE`: безопасная очистка требует пяти явно утверждённых сроков и
подтверждённых replay watermarks. Пока такого runner нет, автоматическая очистка БД выключена,
рост диска мониторится, а это ограничение явно указывается оператору.

## Проверка перед открытием трафика

- `SkyPulse__Mode=Durable` и `ASPNETCORE_ENVIRONMENT=Production`; checked-in
  `LocalFunctional` нельзя считать деплоем.
- `TAP_FULL_NETWORK`, `TAP_SIGNAL_COLLECTION`, `TAP_NO_REPLAY`, `TAP_DISABLE_ACKS`,
  `TAP_WEBHOOK_URL` и `TAP_COLLECTION_FILTERS` отсутствуют/выключены; pprof не опубликован.
- Все три `...Confirmed` настройки exact repository set равны `true`.
- PostgreSQL доступен только по verify-full TLS, exact HBA и active guest firewall rule.
- Private route проходит встроенную проверку имени, режима, размера, profile ID/count/hash.
- Запущены ровно один TAP и один Web/dispatcher; orchestrator не создаёт вторую реплику.
- Есть свежий зашифрованный backup и успешно выполнялся restore drill.
- До runtime growth проверены свободные RAM, PostgreSQL/TAP disk и backup headroom.
