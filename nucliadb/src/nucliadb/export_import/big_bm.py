from nucliadb.common.context import ApplicationContext
from nucliadb.export_import.utils import get_broker_message


async def main():
    context = ApplicationContext("foo")
    await context.initialize()
    bm = await get_broker_message(
        context, "ac79c6bf-afe8-4c31-be87-4c72df70d209", "56c1957b80e791311752c22139cc3dc7"
    )
    with open("big_bm.bin", "wb") as f:
        f.write(bm.SerializeToString())
