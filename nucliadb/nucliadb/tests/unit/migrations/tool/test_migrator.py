from nucliadb.migrations.tool import migrator


def test_get_migrations():
    migrations = migrator.get_migrations()
    assert len(migrations) == 1
    assert migrations[0].version == 1
    assert migrations[0].module.__name__ == "nucliadb.migrations.noop_1"
