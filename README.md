# yandex-backup
## Setup
Для работы скрипта, необходим api-token от Яндекса. Для начала необходим зарегистрировать новое приложение на:

> https://oauth.yandex.ru/client/new

Для работы скрипта нужна только секция "Яндекс.Диск REST API", выделите здесь все. После завершения регистрации вам выдадут ID, вставьте его в следующую ссылку и перейдите по ней:

> https://oauth.yandex.ru/authorize?response_type=token&client_id=<ID приложения>

И вот тут у вас появится токен, запишите его в файл .env, заодно в нем укажите начальную папку, в которой будут храниться ваши бекапы:

> TOKEN = "Длииииииииииииииииный токен"</br>
> REMOTE_DIR = "/Some/folder"

Далее зависимости, я использую Pipenv:

> pipenv sync

Далее, через backup_list.yml нужно указать файлы, папки и их атрибуты хранения и архивации
