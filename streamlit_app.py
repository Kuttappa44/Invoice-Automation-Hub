import io
import re
import time
import os
import base64
import traceback
from datetime import datetime
from contextlib import redirect_stdout

import streamlit as st


EXCEL_LINK = "https://drive.google.com/file/d/1Nb-K8ROUun1qI4QajNMTTlk6Pi03AlbT/view"
LOGO_PATH = "hummingbird_logo.png"  # place your logo image in project root with this name
DRIVE_FOLDERS = {
    "KUMAR": "https://drive.google.com/drive/folders/13srECQoBycz0IJIQjX8YW0K7RBkqbT4B",
    "LAKSHMI": "https://drive.google.com/drive/folders/1PgcQlbb-ZAr3Fm8PJP2Vc2yizL53amCI",
    "MOKSHITHA": "https://drive.google.com/drive/folders/1shQR48yB_LTG8B4aqJLkfFgIQJ7oq7Yu",
    "SANDHYA": "https://drive.google.com/drive/folders/19dU_CD8SdZYO8jMFGhv6VL4qiSsOSbDR",
    "UNASSIGNED": "https://drive.google.com/drive/folders/1p2t900g0EoSoE_6XMyylGe2TSFZd7CB3",
}


@st.cache_data(show_spinner=False)
def get_links():
    return EXCEL_LINK, DRIVE_FOLDERS


def run_processor_and_capture_logs():
    """Import and run the final invoice processor capturing stdout logs."""
    log_stream = io.StringIO()
    start = time.time()
    try:
        with redirect_stdout(log_stream):
            from invoice_processor_final import process_invoices
            process_invoices()
    except Exception as e:
        print(f"❌ Streamlit runner error: {e}")
    end = time.time()
    logs = log_stream.getvalue()
    duration_s = end - start
    return logs, duration_s


def parse_folder_uploads(logs: str):
    """Return list of (folder, filename) for uploaded PDFs and a per-folder count."""
    uploads = []
    by_folder = {k: 0 for k in ("KUMAR", "LAKSHMI", "MOKSHITHA", "SANDHYA", "UNASSIGNED")}
    for line in logs.splitlines():
        # Matches: "☁️  Uploaded PDF to Google Drive: FOLDER/FILENAME.pdf"
        m = re.search(r"Uploaded PDF to Google Drive:\s*(\w+)\/(.+)$", line)
        if m:
            folder = m.group(1)
            filename = m.group(2).strip()
            uploads.append((folder, filename))
            if folder in by_folder:
                by_folder[folder] += 1
    return uploads, by_folder


def main():
    st.set_page_config(page_title="Invoice Automation Hub", page_icon="📧", layout="wide")

    # Fancy font + styled title with optional logo on the left
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700;900&family=Poppins:wght@500;600&display=swap');
        .app-title {
            font-family: 'Playfair Display', serif;
            font-size: 2.6rem;
            line-height: 1.2;
            margin: 0 0 0.25rem 0;
            background: linear-gradient(90deg, #5b86e5 0%, #36d1dc 100%);
            -webkit-background-clip: text;
            background-clip: text;
            color: transparent;
        }
        .app-subtitle {
            font-family: 'Poppins', sans-serif;
            font-size: 0.95rem;
            color: #6b7280;
            margin: 0 0 1rem 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if os.path.exists(LOGO_PATH):
        try:
            with open(LOGO_PATH, "rb") as f:
                b64 = base64.b64encode(f.read()).decode("utf-8")
            st.markdown(
                f"""
                <style>
                .app-header {{
                    display: flex;
                    align-items: center;
                    gap: 14px;
                    margin-bottom: 12px;
                }}
                .app-logo {{
                    height: 64px;
                    width: auto;
                }}
                </style>
                <div class="app-header">
                  <img class="app-logo" src="data:image/png;base64,{b64}" alt="Logo" />
                  <div>
                    <h1 class="app-title">Invoice Automation Hub</h1>
                    <p class="app-subtitle">Process, upload, and track invoices with one click</p>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        except Exception:
            # Fallback to old rendering if any error occurs
            st.markdown(
                """
                <h1 class="app-title">Invoice Automation Hub</h1>
                <p class="app-subtitle">Process, upload, and track invoices with one click</p>
                """,
                unsafe_allow_html=True,
            )
    else:
        # Show uploader + title when logo not present
        st.caption(f"Logo file not found at: `{os.path.abspath(LOGO_PATH)}`")
        uploader = st.file_uploader("Upload logo", type=["png", "jpg", "jpeg"], key="logo_uploader")
        if uploader is not None:
            try:
                data = uploader.read()
                if not data:
                    raise ValueError("Uploaded file is empty")
                with open(LOGO_PATH, "wb") as f:
                    f.write(data)
                st.success("Logo saved. Reloading...")
                try:
                    st.rerun()
                except Exception:
                    st.experimental_rerun()
            except Exception as e:
                st.error(f"Failed to save logo: {e}")
                st.code(traceback.format_exc())
        st.markdown(
            """
            <h1 class="app-title">Invoice Automation Hub</h1>
            <p class="app-subtitle">Process, upload, and track invoices with one click</p>
            """,
            unsafe_allow_html=True,
        )

    # Quick links directly under the title
    excel_link, drive_links = get_links()
    with st.container():
        st.markdown(f"**[Invoice Report]({excel_link})**")
        cols = st.columns(5)
        names = list(drive_links.keys())
        for idx, name in enumerate(names):
            with cols[idx % 5]:
                st.markdown(f"**[{name}]({drive_links[name]})**")

    st.divider()

    # Controls
    if "running" not in st.session_state:
        st.session_state.running = False
    if "last_run" not in st.session_state:
        st.session_state.last_run = None
    if "last_uploads" not in st.session_state:
        st.session_state.last_uploads = []
    if "last_by_folder" not in st.session_state:
        st.session_state.last_by_folder = {}
    if "last_duration" not in st.session_state:
        st.session_state.last_duration = 0.0

    run_btn = st.button("▶️ Run Processor", disabled=st.session_state.running)

    if run_btn and not st.session_state.running:
        st.session_state.running = True
        with st.spinner("Running processor, please wait..."):
            logs, duration = run_processor_and_capture_logs()
        st.session_state.running = False

        uploads, by_folder = parse_folder_uploads(logs)
        st.session_state.last_uploads = uploads
        st.session_state.last_by_folder = by_folder
        st.session_state.last_duration = duration
        st.session_state.last_run = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Results view (no verbose logs)
    st.subheader("Last Run Summary")
    if st.session_state.last_run:
        st.write(f"Run at: {st.session_state.last_run} (took {st.session_state.last_duration:.1f}s)")
        # Show folder counts
        cols = st.columns(5)
        order = ["KUMAR", "MOKSHITHA", "LAKSHMI", "SANDHYA", "UNASSIGNED"]
        for i, folder in enumerate(order):
            count = st.session_state.last_by_folder.get(folder, 0)
            with cols[i % 5]:
                st.metric(folder, count)

        # Show which folder each invoice went to
        st.write("")
        st.markdown("**Uploaded this run:**")
        if st.session_state.last_uploads:
            for folder, filename in st.session_state.last_uploads:
                st.write(f"- {filename} → `{folder}`")
        else:
            st.info("No uploads in the last run.")
    else:
        st.info("Click 'Run Processor' to start a processing run.")


if __name__ == "__main__":
    main()


