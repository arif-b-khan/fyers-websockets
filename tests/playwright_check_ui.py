from playwright.sync_api import sync_playwright
import time
import json
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "playwright-output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

URL = os.environ.get('APP_URL', 'http://127.0.0.1:5000')

print(f"Starting Playwright against: {URL}")

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    console_logs = []

    def on_console(msg):
        try:
            text = msg.text
        except Exception:
            text = str(msg)
        entry = f"{msg.type}: {text}"
        console_logs.append(entry)
        print("CONSOLE:", entry)

    page.on("console", on_console)
    page.on("pageerror", lambda exc: print("PAGEERROR:", exc))

    try:
        page.goto(URL, timeout=15000)
    except Exception as e:
        print(f"Failed to load page {URL}: {e}")

    # Try to inject a Socket.IO hook that logs market_depth events to console
    try:
        page.evaluate("""
            () => {
                window.__playwright_socket_log = [];
                if (!window.io && !window.socket) {
                    console.warn('PW-HOOK: Socket.IO client not found on page');
                    return;
                }
                try {
                    const create = window.socket ? (() => window.socket) : window.io;
                    const s = create();
                    window.__pw_socket = s;

                    s.on('connect', () => {
                        console.log('PW: socket connected', s.id || '(no id)');
                    });

                    s.on('market_depth', (data) => {
                        try {
                            const json = JSON.stringify(data);
                            console.log('PW: market_depth_received', json);
                            window.__playwright_socket_log.push({ts: Date.now(), data});
                        } catch (e) {
                            console.log('PW: market_depth_received (non-serializable)');
                        }
                    });

                    s.on('connect_error', (err) => { console.error('PW: socket connect_error', err); });
                } catch (err) {
                    console.error('PW-HOOK: error while creating socket hook', err);
                }
            }
        """)
    except Exception as e:
        print("Failed to inject PW hook:", e)

    # Wait up to 15s for evidence of socket connection or market_depth events
    start = time.time()
    found_event = False
    while time.time() - start < 15:
        # check collected console messages
        for l in console_logs:
            if 'PW: socket connected' in l or 'PW: market_depth_received' in l or 'Frontend received market_depth' in l or '📥 Frontend received market_depth' in l:
                found_event = True
                break
        if found_event:
            break
        time.sleep(0.5)

    # Capture DOM snapshot for a few key elements
    ids = ['total-bid-qty','total-ask-qty','visible-bid-qty','visible-ask-qty','last-price','bid-ask-ratio','bids','asks']
    dom = {}
    for _id in ids:
        try:
            el = page.query_selector(f'#{_id}')
            if el:
                # For tables, also capture child row count
                text = el.inner_text()
                dom[_id] = text
                if _id in ('bids','asks'):
                    dom[f'{_id}_rows'] = len(el.query_selector_all('tr'))
            else:
                dom[_id] = None
        except Exception as e:
            dom[_id] = f"ERROR: {e}"

    # Save output artifacts
    try:
        screenshot_path = os.path.join(OUTPUT_DIR, 'page.png')
        page.screenshot(path=screenshot_path, full_page=True)
        print('Saved screenshot to', screenshot_path)
    except Exception as e:
        print('Screenshot failed:', e)

    try:
        html_path = os.path.join(OUTPUT_DIR, 'page.html')
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(page.content())
        print('Saved page HTML to', html_path)
    except Exception as e:
        print('Saving HTML failed:', e)

    try:
        console_path = os.path.join(OUTPUT_DIR, 'console.log')
        with open(console_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(console_logs))
        print('Saved console log to', console_path)
    except Exception as e:
        print('Saving console log failed:', e)

    try:
        dom_path = os.path.join(OUTPUT_DIR, 'dom.json')
        with open(dom_path, 'w', encoding='utf-8') as f:
            json.dump(dom, f, indent=2)
        print('Saved DOM snapshot to', dom_path)
    except Exception as e:
        print('Saving DOM snapshot failed:', e)

    print('\n==== SUMMARY ====')
    print('Found evidence of socket or market_depth event in console logs:', found_event)
    print('DOM snapshot:', json.dumps(dom, indent=2))

    browser.close()

print('Playwright run complete. Check tests/playwright-output for artifacts.')
