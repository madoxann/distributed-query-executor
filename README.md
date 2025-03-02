# Обернуть сервис SQLite с помощью Apache Arrow Flight SQL

Это реализация простого клиент-серверного приложения на C++, где сервер отвечает на SQL-запросы, по протоколу Apache Arrow Flight SQL. Сервер читает данные из базы SQLite, а клиент отправляет SQL-запросы. Проект использует Conan для управления зависимостями и CMake для сборки.

## Требования
- **Компилятор с поддержкой C++20**
- **CMake** (>=3.15)
- **Conan**
- **SQLite3**
- **Apache Arrow** (включая Arrow Flight и Arrow Flight SQL) (пришлось собрать самому и положить в /libs, Arrow Flight SQL сейчас не получается собрать из conan)
- **Boost**
- **Google Test**

## Установка и настройка окружения

Установите все зависимости через Conan:
   ```bash
   conan install . --build=missing
   ```

## Сборка проекта

Так как github не пропускает файлы более 100Мб, а из пакета arrow в conan не получается собрать Arrow Flight SQL, то необходимо сделать
   ```bash
  cd libs/lib
  unzip arrow.zip 
  rm arrow.zip 
  cd ../../
  ```

Для сборки понадобится CMake:
   ```bash
   cd build
   cmake .. -DCMAKE_TOOLCHAIN_FILE=Release/generators/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release
   cmake --build .
   ```

Это создаст три исполняемых файла:
- `server`: для запуска сервера
- `client`: для запуска клиента
- `tests`: для запуска тестов

## Запуск сервера

После сборки проекта, запустите сервер командой:
```bash
./server --database-filename="mdxn"
```

Сервер начнет слушать входящие подключения на порту по умолчанию (его можно изменить).

## Запуск клиента

Чтобы выполнить SQL-запрос к серверу, запустите клиент:
```bash
./client --query "create table Groups (group_id int, group_no char(6));"
```

Клиент подключится к серверу и выполнит предопределенный SQL-запрос. Результаты запроса будут выведены на экран.

## Запуск тестов

```bash
./tests
```

Тесты проверяют взаимодействия между клиентом и сервером, а также корректность обработки простых SQL-запросов.

## Структура проекта

- `src/bridge/`: код, отвечающий за взаимодействие между клиентом и сервером через Arrow Flight SQL.
- `src/client/`: код клиента, который подключается к серверу и отправляет SQL-запросы.
- `src/server/`: код сервера.
- `test/`: тесты на основе Google Test.
