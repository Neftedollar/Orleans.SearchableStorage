# Передача локальному агенту

Цель — развернуть первый калибровочный стенд SkyPulse на двух CX53, не выдавая
его за HA или пройденную квалификацию.

## Жёсткие границы

1. Один контейнер SkyPulse, один TAP. Не поднимать вторую реплику приложения и
   не делать blue/green.
2. SkyPulse и TAP работают через `network_mode: host`, но слушают только
   `127.0.0.1`. Значение `ws://tap:2480/channel` неверно и будет отклонено;
   нужно `ws://127.0.0.1:2480/channel`.
3. PostgreSQL через host network слушает только приватный IP. Hetzner Cloud
   Firewall не фильтрует private Cloud Network: обязательный guest nftables и
   `pg_hba.conf` разрешают 5432 лишь с приватного IP app-хоста.
4. Все внешние образы задаются как `name@sha256:...`; `latest` запрещён.
   Локальные app/TAP образы собираются из конкретного Git commit, а их image ID
   записывается в `runtime/images.env`.
5. Секретов в `.env`, JSON, Compose, аргументах команд и Git быть не должно.
   Entry points читают отдельные файлы из read-only mounts.
6. Каталог private routes — настоящий каталог `0700`; оба соседних файла
   `routing.private.manifest.json` и `routing.private.ndjson` — обычные файлы
   `0600`, без symlink, владелец UID 10001.
7. Не использовать production-БД для integration tests: тесты удаляют схему
   `skypulse`.
8. 503 от `/health` во время восстановления — нормальная readiness-семантика,
   а не причина рестарта.
9. Каталог handoff должен быть закоммичен именно как
   `qualification/SkyPulse/deploy/hetzner-cx53`. `DEPLOYMENT_ID` — полный SHA
   этого нового commit. Standalone-копия или старый SHA не являются build
   input: `build-images.sh` извлекает только точное дерево Git.

## Что локальный агент должен заполнить

- точный commit/branch для релиза;
- digest образов .NET SDK/runtime, PostgreSQL и Caddy;
- домен либо решение работать через SSH tunnel;
- публичный и приватный адреса двух хостов;
- имена non-loopback интерфейсов, которые реально владеют этими private IP
  (`ip -4 -o address show`), отдельно для app и PostgreSQL;
- профиль: ID, версия, cap и точный lowercase prefix SHA-256;
- постоянный ненулевой `SourceInstanceId`;
- пути к `corpus.manifest.json` + `accounts.ak32`;
- пути к `routing.private.manifest.json` + `routing.private.ndjson`;
- при необходимости заранее подготовленные growth profiles.

`SourceInstanceId` нельзя генерировать при каждом рестарте. Он живёт столько же,
сколько TAP database/namespace. При потере TAP DB старый UUID использовать
нельзя: нужен контролируемый новый namespace/rebuild.

## Порядок

1. Проверить checkout и `git status`; работать в отдельной ветке/релизном теге.
2. Сгенерировать на доверенной машине и review/commit-нуть OpenTofu provider
   lock для Linux/AMD64. Без `.terraform.lock.hcl` bundle намеренно не проходит
   локальную проверку. Затем выполнить `scripts/verify-bundle.sh` и
   `scripts/verify-source.sh`.
3. Создать секреты и TLS на доверенной машине; разнести минимальные наборы.
4. Выполнить только точный read-only-lock/off-repo-state workflow из
   `infra/README.md`: `tofu init -lockfile=readonly`, validate и plan в закрытый
   state root. План просмотреть вручную, затем применить локально только после
   подтверждения владельца. Generic backend-less `tofu init` запрещён.
5. Установить Docker Engine, Buildx и Compose v2 из доверенного источника на
   обоих хостах; записать версии.
6. Установить на обоих хостах checked journald policy и соответствующий
   `install-*-firewall.sh`, имея открытую Hetzner Console. Installer отключает
   socket-activation SSH и требует classic `ssh.service`, чтобы не создавать
   systemd boot cycle; проверить второй SSH login до выхода из первой сессии и
   сделать reboot-test. Затем запустить PostgreSQL и
   проверить TLS `verify-full` от app-хоста отдельно для
   обеих ролей/БД.
7. На отдельной тестовой БД выполнить все PostgreSQL integration tests; в TRX
   должно быть не меньше 34 тестов, 0 failed и 0 notExecuted.
8. Получить реальный corpus, сделать freeze/deep verify и private route export.
9. Собрать app/TAP, сверить хэши TAP binary и NuGet package.
10. Запустить app stack и ждать readiness/полной синхронизации без автоматических
    рестартов по 503. Затем на host A остановить Caddy/Web/TAP, на host B снять
    согласованный `backup-postgres.sh` с обеими confirmation-переменными, скопировать
    его через утверждённый encrypted off-host канал и успешно выполнить
    `drill-postgres-backup.sh`. Только после этого снова запустить app и считать
    поток operational.
11. Выполнить smoke через фактический TLS proxy, включая SSE heartbeat; до
    backup/drill публичный proxy не открывать.
12. Зафиксировать Git SHA, image IDs/digests, хэши конфигурации/корпуса/routes и
    результаты, но не копировать DIDs или private artifacts в evidence.
13. Перед 100K/1M проверить RAM, PostgreSQL/WAL/disk, lag, rebuild time и
    выполнить `drill-postgres-backup.sh`. Только затем вызывать
    `request-growth.sh`.
14. После успешного роста и полной синхронизации остановить app/TAP, обновить
    `EXPECTED_ACTIVE_PROFILE_ID/CAP` в `.env` на обоих хостах (не менять
    `EXPECTED_BASE_*`), выпустить новые paired release records, backup и
    пройти restore drill до открытия трафика.

## Когда остановиться

Остановиться и не пытаться «обойти» проверку, если:

- образ не закреплён digest или SDK не `10.0.303`;
- package/canonical manifest hash не совпал;
- Docker пытается передать private routes в build context;
- corpus/profile/routes не совпадают по ID/cap/prefix SHA;
- секрет является placeholder, содержит CR/LF или доступен посторонним;
- TLS не проходит `verify-full`;
- запущено больше одного SkyPulse/TAP;
- есть незавершённый рост профиля перед обновлением;
- нет согласованной физической копии PostgreSQL перед несовместимым обновлением;
- нет отдельно реализованного и restore-tested зашифрованного off-host recovery
  set с PG backup, соответствующими DB secrets, corpus/routes/TLS и двумя
  release records — в таком состоянии допустима только ограниченная
  калибровка, не длительный production;
- не реализовано и не проверено шифрование данных на дисках хостов: штатные
  root-диски из OpenTofu не считаются доказательством encryption-at-rest, а
  реальные routes/PGDATA/secrets/backups до отдельного решения размещать нельзя;
- кто-то предлагает `docker compose down -v`, ad-hoc DELETE или отдельный откат
  только одной из двух БД.
