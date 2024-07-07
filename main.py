import asyncio
import os
import re
from email import message_from_bytes
from email.utils import getaddresses, parseaddr, parsedate_to_datetime
from hashlib import sha256
from itertools import batched
from json import dumps

import aioimaplib
from cloudflare import AsyncCloudflare as Cloudflare
from dotenv import dotenv_values

if os.name == "nt":
    import winloop as uvloop
elif os.name == "posix":
    import uvloop


def clean(s: str | None) -> str | None:
    if s is None:
        return None
    _s = s.strip("\r\n\t").strip("\r\n ")
    return _s.replace("\r\n\t", " ").replace("\r\n ", " ")


async def check_mailbox(host: str, user: str, password: str):
    imap_client = aioimaplib.IMAP4_SSL(host)
    try:
        await imap_client.wait_hello_from_server()
        await imap_client.login(user, password)
        await rw(imap_client, "Inbox")
        await rw(imap_client, "Sent")

    finally:
        await imap_client.logout()


async def rw(client: aioimaplib.IMAP4_SSL, box: str):
    _, data = await client.select(box)  # _ = OK
    size = int(data[0].split()[0])
    _, fetch_data = await client.fetch(
        f"{size - 7}:*", "(BODY.PEEK[])"
    )  # I'm too lazy to implement stream
    # print(typ, fetch_data)
    # for i in fetch_data:
    #     print(i[:50])
    # exit()
    emails = [
        (int(i.split()[0]), message_from_bytes(j))
        for (i, j, _) in batched(fetch_data[:-1], 3)
    ]
    # print(emails[0][0])
    # print(emails[0][1].items())
    # for e in emails:
    #     print("ROUND")
    #     print(e[1].is_multipart())
    #     if e[1].is_multipart():
    #         for i in e[1].walk():
    #             print(i.get_content_type())
    #             print("x", i.get_payload())
    #         print(e[1].get_body())

    # exit()

    async with asyncio.TaskGroup() as tg:
        for email in emails:
            i, j = email
            msg_id = j["Message-ID"]
            if len(set(msg_id) & set("\t\n\r ")) != 0:
                print("Python Parser sucks")
            _msg_id = re.search(r"<.+?>", msg_id)[0]
            from_person = parseaddr(j["From"])
            all_recipients = getaddresses(j.get_all("To", []) + j.get_all("Cc", []))
            sha = sha256(_msg_id.encode()).hexdigest()
            # boto3 is not async
            # s3_client.upload_fileobj(
            #     io.BytesIO(j.as_bytes()), "assets", f"discuss/{sha}.eml"
            # )
            tg.create_task(
                cf_client.d1.database.query(
                    database_id=config["CF_DB_ID"],
                    account_id=config["CF_ACCOUNT_ID"],
                    sql="INSERT OR IGNORE INTO GlobalMessages (Folder, MessageID, MessageIDHash, Epoch, InReplyTo, SubjectLine, Author, Recipients, RAWMessage, FolderSerial) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    params=[
                        box,
                        _msg_id,
                        sha,
                        parsedate_to_datetime(j["Date"]).timestamp(),
                        clean(j["In-Reply-To"]),
                        clean(j["Subject"]),
                        dumps({"name": from_person[0], "address": from_person[1]}),
                        dumps([
                            {"name": i[0], "address": i[1]} for i in all_recipients
                        ]),
                        "0",
                        str(i),
                    ],
                )
            )
            tg.create_task(
                cf_client.d1.database.query(
                    database_id=config["CF_DB_ID"],
                    account_id=config["CF_ACCOUNT_ID"],
                    # Well, old message will not be updated
                    sql="UPDATE GlobalMessages SET FolderSerial = ? WHERE MessageIDHash = ?",
                    params=[
                        str(i),
                        sha,
                    ],
                )
            )
        tg.create_task(
            cf_client.d1.database.query(
                database_id=config["CF_DB_ID"],
                account_id=config["CF_ACCOUNT_ID"],
                sql="DELETE FROM GlobalMessages WHERE Folder = ? AND FolderSerial > ?",
                params=[box, str(size)],
            )
        )
        for i, j in emails:
            msg_id = j["Message-ID"]
            _msg_id = re.search(r"<.+?>", msg_id)[0]
            tg.create_task(
                cf_client.d1.database.query(
                    database_id=config["CF_DB_ID"],
                    account_id=config["CF_ACCOUNT_ID"],
                    sql="DELETE FROM GlobalMessages WHERE Folder = ? AND MessageID = ? AND FolderSerial <> ?",
                    params=[box, _msg_id, str(i)],
                )
            )


config = dotenv_values(".env")
cf_client = Cloudflare(api_token=config["CF_TOKEN"])
# s3_client = boto3.client(
#     service_name="s3",
#     endpoint_url=f'https://{config["CF_ACCOUNT_ID"]}.r2.cloudflarestorage.com',
#     aws_access_key_id=config["R2_ACCESS_KEY_ID"],
#     aws_secret_access_key=config["R2_SECRET_ACCESS_KEY"],
#     region_name="apac",  # Must be one of: wnam, enam, weur, eeur, apac, auto
# )


async def main():
    await check_mailbox(
        config["IMAP_HOST"], config["IMAP_USERNAME"], config["IMAP_PASSWORD"]
    )


if __name__ == "__main__":
    uvloop.run(main())
