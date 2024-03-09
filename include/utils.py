import trio
from selenium import webdriver


async def create_driver_trio():
    return await trio.to_thread.run_sync(
        webdriver.Remote,
        "http://selenium:4444",
        True,
        None,
        webdriver.ChromeOptions(),
    )


async def create_driver() -> webdriver.Remote:
    return webdriver.Remote(
        "http://selenium:4444",
        True,
        None,
        webdriver.ChromeOptions(),
    )
