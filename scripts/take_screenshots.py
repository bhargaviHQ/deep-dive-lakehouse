"""Capture screenshots of each Optimization Lab tab using Playwright.

Requires: pip install playwright && playwright install chromium
Run after the dashboard is already running: streamlit run app/dashboard.py
"""
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright

BASE_URL = "http://localhost:8501"
OUT = Path(__file__).resolve().parents[1] / "screenshots"
OUT.mkdir(exist_ok=True)


async def wait_for_spinner(page):
    """Wait for any running spinner to disappear."""
    try:
        await page.wait_for_selector("[data-testid='stSpinner']", timeout=3000)
        await page.wait_for_selector("[data-testid='stSpinner']", state="hidden", timeout=30000)
    except Exception:
        pass
    await page.wait_for_timeout(800)


async def click_tab(page, label: str):
    await page.get_by_role("tab", name=label).click()
    await page.wait_for_timeout(600)


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport={"width": 1400, "height": 900})

        print("Loading app…")
        await page.goto(BASE_URL, wait_until="networkidle")
        await page.wait_for_timeout(2000)

        # ── Tab 1: Partitioning ──────────────────────────────────────────
        print("Tab 1: Partitioning…")
        await click_tab(page, "Partitioning")
        await page.wait_for_timeout(500)

        # Fill date inputs
        date_inputs = page.locator("input[type='text']")
        await date_inputs.nth(0).click(click_count=3)
        await date_inputs.nth(0).type("2023/06/15")
        await date_inputs.nth(0).press("Enter")
        await page.wait_for_timeout(200)
        await date_inputs.nth(1).click(click_count=3)
        await date_inputs.nth(1).type("2023/06/17")
        await date_inputs.nth(1).press("Enter")
        await page.wait_for_timeout(400)

        await page.get_by_role("button", name="Run Comparison").click()
        await wait_for_spinner(page)
        await page.wait_for_timeout(1200)
        await page.screenshot(path=str(OUT / "tab1_partitioning.png"), full_page=True)
        print("  saved tab1_partitioning.png")

        # ── Tab 2: Compression ───────────────────────────────────────────
        print("Tab 2: Compression…")
        await click_tab(page, "Compression")
        await page.wait_for_timeout(500)
        await page.get_by_role("button", name="Run Comparison").click()
        await wait_for_spinner(page)
        await page.wait_for_timeout(1200)
        await page.screenshot(path=str(OUT / "tab2_compression.png"), full_page=True)
        print("  saved tab2_compression.png")

        # ── Tab 3: Pre-aggregation ───────────────────────────────────────
        print("Tab 3: Pre-aggregation…")
        await click_tab(page, "Pre-aggregation")
        await page.wait_for_timeout(800)
        # Streamlit selectbox: click the box then pick the option
        selectbox = page.locator("[data-testid='stSelectbox']").first
        await selectbox.click()
        await page.wait_for_timeout(400)
        await page.get_by_text("Vendor daily efficiency").click()
        await page.wait_for_timeout(400)
        await page.get_by_role("button", name="Run Comparison").click()
        await wait_for_spinner(page)
        await page.wait_for_timeout(1200)
        await page.screenshot(path=str(OUT / "tab3_preagg.png"), full_page=True)
        print("  saved tab3_preagg.png")

        # ── Tab 4: Star Schema ───────────────────────────────────────────
        print("Tab 4: Star Schema…")
        await click_tab(page, "Star Schema")
        await page.wait_for_timeout(1500)  # redundancy chart renders on load
        await page.screenshot(path=str(OUT / "tab4_star_schema.png"), full_page=True)
        print("  saved tab4_star_schema.png")

        await browser.close()
        print("Done. Screenshots saved to:", OUT)


asyncio.run(main())
