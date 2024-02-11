import psycopg2


with psycopg2.connect(database="netology_db", user="postgres", password="postgres") as conn:
    with conn.cursor() as cur:
        # удаление таблиц (в начале для возможности удалить все данные и заново наполнять)
        # cur.execute("""
        # DROP TABLE homework;
        # DROP TABLE course;
        # """)

        # создание таблиц
        cur.execute("""
        CREATE TABLE IF NOT EXISTS course(
            id SERIAL PRIMARY KEY,
            name VARCHAR(40) UNIQUE
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS homework(
            id SERIAL PRIMARY KEY,
            number INTEGER NOT NULL,
            description TEXT NOT NULL,
            course_id INTEGER NOT NULL REFERENCES course(id)
        );
        """)
        conn.commit()  # фиксируем в БД

        # наполнение таблиц (C из CRUD)
        cur.execute("""
        INSERT INTO course(name) VALUES('Python');   #SERIAL ставит id автоматически?
        """)
        conn.commit()  # фиксируем в БД

        cur.execute("""
        INSERT INTO course(name) VALUES('Java') RETURNING id;       #результат наполнения не виден сразу для просмотра RETURUNG. Например, вернуть id для значения 'Java' в  столбце name  таблицы course
        """)
        print(cur.fetchone())  # запрос данных автоматически зафиксирует изменения = conn.commit()

        cur.execute("""
        INSERT INTO homework(number, description, course_id) VALUES(1, 'простое дз', 1);
        """)
        conn.commit()  # фиксируем в БД

        # извлечение данных (R из CRUD)
        cur.execute("""
        SELECT * FROM course;
        """)
        print('fetchall', cur.fetchall())  # извлечь все строки

        cur.execute("""
        SELECT * FROM course;
        """)
        print(cur.fetchone())  # извлечь первую строку (аналог LIMIT 1)

        cur.execute("""
        SELECT * FROM course;
        """)
        print(cur.fetchmany(3))  # извлечь первые N строк (аналог LIMIT N)

        cur.execute("""
        SELECT name FROM course;
        """)
        print(cur.fetchall())     #извлекает список кортежей

        cur.execute("""
        SELECT id FROM course WHERE name='Python';
        """)
        print(cur.fetchone())       #извлекает один кортеж(!не список значений, а один объект - кортеж)

        cur.execute("""
        SELECT id FROM course WHERE name='{}';
        """.format("Python"))  # плохо - возможна SQL инъекция
        print(cur.fetchone())

        cur.execute("""
        SELECT id FROM course WHERE name=%s;           #%s(элемент заполнитель) - рлдстановка вместо параметров - для безопасной передачи
        """, ("Python",))  # хорошо, обратите внимание на кортеж, т.е. если передаем один параметр, то в скобках и после ставим ","
        print(cur.fetchone())


        -- ФУНКЦИЯ для извлечения данных: узнать идентификатор курса по его имени. С её помощью передаем str и получаем id.
        def get_course_id(cursor, name: str) -> int:
            cursor.execute("""
            SELECT id FROM course WHERE name=%s;              #передаем запрос с подстановкой %s(элемент заполнитель)
            """, (name,))                                     #вторым аргументом передаем кортеж-параметр
            return cur.fetchone()[0]                          #хотим получить запись и достать нулевой элемент, т.к. возвращается кортеж объект.
        python_id = get_course_id(cur, 'Python')              #использование функции: вызываем функцию и передаем текущий курсор(который может выполнять SQLзапросы) и параметр для выполнения
        print('python_id', python_id)

        cur.execute("""
        INSERT INTO homework(number, description, course_id) VALUES(%s, %s, %s);
        """, (2, "задание посложнее", python_id))
        conn.commit()  # фиксируем в БД

        cur.execute("""
        SELECT * FROM homework;         #если не укажем конкретные столбцы, которые хотим получить из таблицы и поставим *
        """)
        print(cur.fetchall())           #то вернет список всех кортежей со всеми столбцами

        # обновление данных (U из CRUD)
        cur.execute("""
        UPDATE course SET name=%s WHERE id=%s;    # %s - подстановка вместо параметров, для безопасной передачи параметров
        """, ('Python Advanced', python_id))      # передача котрежей параметров
        cur.execute("""
        SELECT * FROM course;
        """)
        print(cur.fetchall())  # запрос данных автоматически зафиксирует изменения

        # удаление данных (D из CRUD)
        cur.execute("""
        DELETE FROM homework WHERE id=%s;
        """, (1,))
        cur.execute("""
        SELECT * FROM homework;
        """)
        print(cur.fetchall())  # запрос данных автоматически зафиксирует изменения

conn.close()

