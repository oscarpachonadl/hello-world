import db_ingest_test.ingest_data_db_test as db


if __name__ == '__main__':
    # insert row by row
    # db.data_insert_test(10000, 500) 

    # insert many rows in one insert
    db.data_insert_test_many(10000, 500)
