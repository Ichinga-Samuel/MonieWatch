from .env import env

TORTOISE_ORM = {
    "connections": {
        "default": {
            "engine": "tortoise.backends.mysql",
            "credentials": {
                "host": env.DB_HOST,
                "user": env.DB_USER,
                "port": env.DB_PORT,
                "password": env.DB_PASSWORD,
                "database": env.DB_NAME,
                "ssl": False
            }
        },
    },
    "apps": {
        "models": {
            "models": ["models.tables_orm", "aerich.models"],
            "default_connection": "default"
        }
    },
    "use_tz": False,
    "timezone": env.DB_TIMEZONE
}
