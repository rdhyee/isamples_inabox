# import sqlite3
# from isb_web.sqlmodel_database import SQLModelDAO


# Manual script to dump and compare records between SESAR and iSamples
if __name__ == "__main__":

    sqlite_dump_file = "/Users/mandeld/iSamples/isamples_docker/isb/isamples_inabox/scripts/sesar_igsns.csv"
    isamples_dump_file = "/Users/mandeld/iSamples/isamples_docker/isb/isamples_inabox/scripts/isamples_sesar_igsns2.csv"
    sqlite_igsns = {}
    isamples_igsns = {}

    with open(sqlite_dump_file, "r") as sqlite_file:
        for line in sqlite_file:
            row = line.split(",")
            sqlite_igsns[row[0]] = row[1]

    linenum = 0
    with open(isamples_dump_file, "r") as isamples_file:
        for line in isamples_file:
            row = line.split(",")
            key = row[0]
            if key in isamples_igsns:
                print("duplicate key")
            isamples_igsns[row[0]] = row[1]
            linenum += 1

    with open("missing_igsns.csv", "a") as missing_file:
        for key, value in sqlite_igsns.items():
            if key not in isamples_igsns:
                missing_file.write(f"{key},{value}")

    # url = "postgresql+psycopg2://isb_writer:@localhost/isb_3"
    # limit = 50000
    # offset = 0
    # keep_going = True
    # written_keys = set()
    # with open("isamples_sesar_igsns2.csv", "a") as isamples_igsn_file:
    #     while keep_going:
    #         sql = f"select id, tcreated from thing order by _id desc limit {limit} offset {offset}"
    #         session = SQLModelDAO(url).get_session()
    #         rs = session.execute(sql).all()
    #         keep_going = len(rs) > 0
    #         offset += limit
    #         for row in rs:
    #             igsn = row._asdict()["id"]
    #             igsn = igsn.removeprefix("IGSN:")
    #             if igsn in written_keys:
    #                 print("wtfbbq")
    #             tcreated = row._asdict()["tcreated"]
    #             isamples_igsn_file.write(f"{igsn},{tcreated}\n")
    #             written_keys.add(igsn)

    # sqlite_connection = sqlite3.connect('/Users/mandeld/Downloads/sesar-20220208.db')
    # cursor = sqlite_connection.cursor()
    # limit = 50000
    # offset = 0
    # keep_going = True
    # records = []
    # while keep_going:
    #     query = f'select loc, lastmod from sitemap order by lastmod desc limit {limit} offset {offset}'
    #     cursor.execute(query)
    #     results = cursor.fetchall()
    #     keep_going = len(results) > 0
    #     offset += limit
    #     with open("sesar_igsns.csv", "a") as sesar_file:
    #         for result in results:
    #             loc = result[0]
    #             lastmod = result[1]
    #             last_slash_index = loc.rfind("/") + 1
    #             after_last_slash = loc[last_slash_index:]
    #             sesar_file.write(f"{after_last_slash},{lastmod}\n")
