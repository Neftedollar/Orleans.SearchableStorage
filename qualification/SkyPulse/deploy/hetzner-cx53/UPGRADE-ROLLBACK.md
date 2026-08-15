# Обновление, откат и восстановление

Текущая топология однопроцессная: Memory index и единственный dispatcher находятся в одном Web
процессе, а index write не имеет внешнего fencing token. Поэтому запрещены blue-green, rolling
upgrade и запуск старой и новой версий одновременно. Обновление выполняется с коротким окном
недоступности; после каждого старта индекс строится заново из PostgreSQL.

## Что зафиксировать до изменения

Сохранить в журнале операции:

- git commit и SHA-256 Web/TAP артефактов;
- точные версии .NET, NuGet package и TAP overlay;
- активные `ProfileId`, `ProfileVersion`, `CorpusCap` и prefix SHA-256;
- `SourceInstanceId` и TAP repository count;
- SHA-256 конфигурационных шаблонов и private route manifests без публикации их содержимого;
- текущие `/health`, corpus-capacity, PostgreSQL schema version, lag/outbox и свободный диск;
- идентификатор последнего успешно проверенного backup/restore point.

Нельзя начинать обновление во время corpus growth, lifecycle transition, аварии диска или без
запаса места для WAL и полной перестройки индекса.

## Обычное обновление

1. Запретить новые запросы роста и включить maintenance/закрыть внешний маршрут Caddy. Не
   останавливать процесс посередине принятого роста.
2. Дождаться стабильного состояния и записать метрики выше.
3. Остановить Web, затем TAP: после этого новые ACK и TAP mutations не принимаются.
4. На host A выполнить `scripts/stop-app.sh` и отдельно подтвердить, что `skypulse-caddy`,
   `skypulse-app` и `skypulse-tap` не запущены. Затем на host B выполнить:

   ```bash
   sudo env \
     SKYPULSE_INGESTION_STOPPED=I_STOPPED_APP_AND_TAP \
     SKYPULSE_APP_HOST_STOPPED=I_VERIFIED_CADDY_APP_AND_TAP_STOPPED_ON_HOST_A \
     scripts/backup-postgres.sh
   ```

   Физическая копия одного PostgreSQL-кластера включает
   обе БД (`skypulse` и `skypulse_tap`) на совместимую точку. Скопировать её зашифрованно
   off-host и проверить закрытый список `SHA256SUMS`. Скрипт создаёт plain-format physical
   backup, выполняет нативный PostgreSQL 17 `pg_verifybackup` и добавляет в покрытый хешем
   identity-файл стабильные base/active capacity, SourceInstanceId, migration set и TAP marker.
   Сохранить рядом оба закрытых release record с app- и DB-хоста. Не удалять data directories и
   старый release.
5. Развернуть новые неизменяемые файлы в новый release directory. Секреты и private routes не
   копировать внутрь release; проверить владельцев и режимы.
6. Применить только новую конфигурацию, прошедшую preflight. `SourceInstanceId` при обычном
   обновлении остаётся прежним.
7. Запустить app stack: Compose поднимает TAP, затем единственный Web-процесс. Локально проверить
   authenticated TAP admin API и repository count.
8. Пока Web `/health` возвращает 503, это ожидаемая перестройка,
   а Caddy остаётся в maintenance.
9. Требовать `/health`: `mode=Durable`, `status=ready`; затем проверить corpus capacity,
   фиксированный набор smoke-запросов, SSE, ingestion/ACK lag и отсутствие новых секретов/DID в
   журналах.
10. Открыть Caddy и наблюдать память, WAL, outbox, TAP retries и query latency. Старый release
    хранить только до завершения окна наблюдения; данные и backup не удалять.

Миграции PostgreSQL применяет Web при старте и затем сравнивает всю схему с reviewed contract.
Любое несоответствие — причина оставить трафик закрытым, а не исправлять схему вручную.

## Когда допустим откат только кода

Code-only rollback допустим лишь если старая версия понимает уже установленную схему, активный
profile и TAP wire contract. Порядок: закрыть Caddy, остановить Web и TAP, вернуть старый
неизменяемый release и его совместимую конфигурацию, запустить TAP, затем Web, дождаться полного
rebuild и выполнить те же проверки.

Нельзя считать наличие старого бинарника доказательством совместимости. Если новая версия успела
применить несовместимую миграцию или изменить durable contract, требуется восстановление данных,
а не запуск старого процесса поверх новой БД.

Corpus cap монотонный: активированный рост нельзя отменить сменой конфигурации. Старая версия
должна знать уже активный профиль и иметь его private route. Иначе routine code rollback
запрещён; возврат к меньшему cap возможен только восстановлением согласованной копии до роста и
считается отдельным data recovery с потерей более новых локальных результатов.

## Восстановление данных

1. Закрыть Caddy и остановить Web, затем TAP.
2. Сохранить повреждённые БД/логи read-only для расследования; не выполнять `DROP`, `TRUNCATE` или
   ручное изменение high-water.
3. Выбрать один подтверждённый restore point и восстановить **обе** БД на совместимую точку
   времени. Независимый откат только TAP или только SkyPulse запрещён.
4. Вернуть соответствующие конфигурацию, `SourceInstanceId`, corpus manifests и private routes;
   восстановить каталогам `0700`, файлам `0600` и правильного владельца.
5. Сначала запустить TAP и проверить marker v3, authenticated endpoints и repository count. Затем
   запустить Web, дождаться rebuild/rolling-window catch-up и `mode=Durable`/`ready`.
6. Выполнить smoke-запросы и проверить ACK/outbox/reconciliation до открытия трафика.

Для воспроизводимого физического восстановления использовать только пустой новый каталог данных:

```bash
scripts/stop-app.sh
scripts/stop-postgres.sh
sudo install -d -m 0700 /srv/skypulse/postgres/restore-empty
# Скопируйте весь release deploy/hetzner-cx53 в отдельный restore-release,
# перейдите в него и измените только его .env: POSTGRES_DATA_DIR=.../restore-empty.
# Скрипты всегда читают .env рядом с собой; отдельная несвязанная копия .env не работает.
sudo env \
  SKYPULSE_RESTORE_CONFIRMATION=I_STOPPED_POSTGRES_AND_SELECTED_AN_EMPTY_DATA_DIRECTORY \
  SKYPULSE_APP_HOST_STOPPED=I_VERIFIED_CADDY_APP_AND_TAP_STOPPED_ON_HOST_A \
  scripts/restore-postgres-backup.sh /absolute/encrypted-and-verified-backup
sudo scripts/preflight-postgres.sh
sudo env \
  SKYPULSE_APP_HOST_STOPPED=I_VERIFIED_CADDY_APP_AND_TAP_STOPPED_ON_HOST_A \
  scripts/up-postgres.sh
```

Restore helper сначала проверяет `SHA256SUMS` и нативный PostgreSQL 17 `pg_verifybackup` над
plain-format backup, точную runtime/capacity/TAP identity и отсутствие посторонних файлов/symlink,
затем копирует весь согласованный cluster tree вместе с `pg_wal` и
восстанавливает владельца и режим самого bind-root/`PGDATA` из точного PostgreSQL image. Он
откажется писать в непустой каталог и
никогда не запускает приложение/прокси автоматически. После старта отдельно проверить наличие
обеих БД и только затем запускать TAP/Web.

Обе confirmation-переменные подтверждают межхостовой факт, который DB-хост сам проверить не
может. Не задавать их, пока Caddy/Web/TAP на host A действительно не остановлены. Restore helper
ставит `.skypulse-restore-pending` до записи cluster bytes и оставляет marker при любой ошибке;
первый успешный semantic `up-postgres.sh` удаляет его.

Memory index отдельно не восстанавливается. Копия его памяти или файлов не является backup.

## Потеря или отдельный откат TAP DB

TAP DB хранит монотонный delivery-ID high-water. Старый снимок может повторно выдать уже
использованный ID с другим событием. Поэтому при утрате TAP DB без согласованной резервной копии:

- немедленно оставить Web и Caddy остановленными;
- не запускать пустую/старую TAP DB со старым `SourceInstanceId`;
- не менять UUID в существующей SkyPulse DB: runtime manifest привязан к нему insert-once;
- считать TAP новым источником, создать новый `SourceInstanceId` и новую SkyPulse operational DB
  либо выполнить отдельно утверждённый полный rebuild/reconciliation;
- заново установить точный private repository set и получить доказательство cardinality перед
  readiness.

Аналогично, потеря SkyPulse DB без подходящей копии не лечится текущим TAP head: уже
подтверждённые outbox rows могли быть удалены. Это новый run/full rebuild, а не обычный restart.

## Ротация секретов

TAP admin password находится одновременно у TAP и Web. Для его ротации закрыть трафик, остановить
Web и TAP, заменить оба значения одной операцией, запустить TAP, затем Web и проверить
authenticated connection. Не оставлять период, когда процессы используют разные пароли.

Growth token меняется с перезапуском Web. У PostgreSQL-роли нет двух одновременно действующих
паролей, поэтому ротация выполняется с остановкой зависимого процесса. Сначала создать один новый
64-hex файл и установить его как mode-0400 `.next` на DB-хосте и на app-хосте с соответствующими
владельцами. Остановить Web для `skypulse_app` либо TAP для `skypulse_tap`. На DB-хосте прочитать
значение в shell-переменную и передать фиксированный `ALTER ROLE ... PASSWORD '<64hex>'` в stdin
локального `psql -U skypulse_admin -d postgres`; пароль не должен быть аргументом процесса, env или
логом (`log_statement=none`). После успешного commit атомарно переименовать оба `.next` в активные
mount-файлы, очистить переменную, запустить только зависимый процесс и проверить TLS-аутентификацию.
При любой ошибке оставить процесс остановленным: старый пароль после `ALTER ROLE` уже недействителен,
а recovery-значением является сохранённый новый `.next`. Пароли Caddy и backup не переиспользовать.

## Условия немедленной остановки

- одновременно обнаружены два Web/dispatcher процесса;
- изменился `SourceInstanceId` без намеренной замены TAP DB;
- TAP доступен не через loopback/wss или PostgreSQL открыт публично;
- schema/manifest/private-route validation не проходит;
- backup не согласован между TAP и SkyPulse;
- в журнале или build context обнаружены секрет, DID route либо пользовательский контент;
- приложение готово только в `LocalFunctional` или `/health` не подтверждает `Durable`.

После любого аварийного восстановления выполнить отдельный restore drill и сохранить только
агрегированные результаты и хеши артефактов как эксплуатационное свидетельство.
