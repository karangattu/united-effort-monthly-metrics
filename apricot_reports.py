import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright


class ApricotReportManager:
    """Manages authentication and report fetching for Apricot."""

    def __init__(self, phpsessid=None, csrftoken=None):
        """
        Initialize the manager with optional session credentials.

        Args:
            phpsessid (str): PHP session ID cookie value
            csrftoken (str): CSRF token cookie value
        """
        self.phpsessid = phpsessid
        self.csrftoken = csrftoken
        self.base_url = "https://apricot.socialsolutions.com"
        self.session = requests.Session()
        if phpsessid and csrftoken:
            self._setup_session()

    def _setup_session(self):
        """Configure the session with headers and cookies."""
        user_agent = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; "
            "rv:143.0) Gecko/20100101 Firefox/143.0"
        )
        accept_header = (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,*/*;q=0.8"
        )
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": accept_header,
            }
        )
        self.session.cookies.set("PHPSESSID", self.phpsessid)
        self.session.cookies.set("CSRFTOKEN", self.csrftoken)

    def login(self, headed=False):
        """
        Login to Apricot and extract session cookies.

        Args:
            headed (bool): If True, runs in headed mode (visible browser)

        Returns:
            tuple: (phpsessid, csrftoken) on success, (None, None) on failure
        """
        load_dotenv()
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=not headed)
            page = browser.new_page()

            try:
                page.goto("https://apricot.socialsolutions.com/auth")

                page.get_by_role("textbox", name="Username").click()
                username_field = page.get_by_role("textbox", name="Username")
                username_field.fill(os.getenv("USERNAME"))

                page.get_by_role("textbox", name="Password").click()
                password_field = page.get_by_role("textbox", name="Password")
                password_field.fill(os.getenv("PASSWORD"))

                page.get_by_role("button", name="Log In to Apricot").click()

                time.sleep(5)

                try:
                    continue_btn = page.get_by_role("button", name="Continue")
                    if continue_btn.is_visible():
                        continue_btn.click()
                        page.wait_for_load_state("networkidle")
                except Exception:
                    msg = "ℹ️  No Continue button found"
                    print(msg)

                time.sleep(2)
                cookies = page.context.cookies()
                phpsessid = None
                csrftoken = None

                for cookie in cookies:
                    if cookie["name"] == "PHPSESSID":
                        phpsessid = cookie["value"]
                    elif cookie["name"] == "CSRFTOKEN":
                        csrftoken = cookie["value"]

                if phpsessid and csrftoken:
                    self.phpsessid = phpsessid
                    self.csrftoken = csrftoken
                    self._setup_session()
                    return phpsessid, csrftoken
                else:
                    print("❌ Could not extract session cookies")
                    return None, None

            except Exception as e:
                print(f"❌ Error during login: {e}")
                import traceback
                traceback.print_exc()
                return None, None

            finally:
                browser.close()

    def fetch_new_clients_monthly(self):
        """Fetch the "New Clients/Month" report."""
        report_url = (
            f"{self.base_url}/report/export/"
            "report_id/78/section_id/384/outputFileType/CSV/"
            "export_totals/section_settings/fast_mode/false/"
            "include_limit_sections/false"
        )
        return self._fetch_report(report_url, "New Clients/Month")

    def fetch_benefits_and_applications(self):
        """Fetch the "Benefits and Applications" report."""
        report_url = (
            f"{self.base_url}/report/export/"
            "report_id/86/section_id/392/outputFileType/CSV/"
            "export_totals/section_settings/fast_mode/false/"
            "include_limit_sections/false"
        )
        return self._fetch_report(report_url, "Benefits and Applications")

    def fetch_housing_applications(self):
        """Fetch the "Housing Applications" report."""
        report_url = (
            f"{self.base_url}/report/export/"
            "report_id/81/section_id/387/outputFileType/CSV/"
            "export_totals/section_settings/fast_mode/false/"
            "include_limit_sections/false"
        )
        return self._fetch_report(report_url, "Housing Applications")

    def _fetch_report(self, url, report_name):
        """
        Generic method to fetch any report from Apricot.

        Args:
            url (str): Full report export URL
            report_name (str): Human-readable report name

        Returns:
            str: Path to the downloaded file or None if failed
        """
        try:

            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            report_name_clean = (
                report_name.lower().replace("/", "_").replace(" ", "_")
            )
            output_file = f"{report_name_clean}.csv"

            output_path = Path(output_file)
            output_path.write_bytes(response.content)

            print(f"✅ Downloaded: {output_path.absolute()}")

            return str(output_path.absolute())

        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching report: {e}")
            return None

    def test_connection(self):
        """Test the connection and credentials validity."""
        print("🔍 Testing connection and credentials...")
        try:
            test_url = f"{self.base_url}/report/run/report_id/78"
            response = self.session.get(test_url, timeout=10)

            if response.status_code == 200:
                print("✅ Connection successful!")
                return True
            else:
                print(f"⚠️  Unexpected status code: {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"❌ Connection failed: {e}")
            return False

    @staticmethod
    def load_and_filter_dataframes(report_paths):
        """
        Load CSV reports into dataframes and filter by previous calendar month.
        Outputs a summary CSV with record counts by report type.

        Args:
            report_paths (dict): Dict with report names as keys and
                                file paths as values

        Returns:
            dict: Dataframes filtered to previous calendar month
        """
        today = datetime.now()
        first_of_current = today.replace(day=1)
        last_of_previous = first_of_current - timedelta(days=1)
        first_of_previous = last_of_previous.replace(day=1)

        month_start = first_of_previous.strftime("%Y-%m-%d")
        month_end = last_of_previous.strftime("%Y-%m-%d")

        dataframes = {}
        summary_data = []

        if report_paths.get("new_clients_monthly"):
            df = pd.read_csv(report_paths["new_clients_monthly"])
            df["Creation Date"] = pd.to_datetime(
                df["Creation Date"], errors="coerce"
            )
            df_filtered = df[
                (df["Creation Date"].dt.strftime("%Y-%m-%d")
                 >= month_start)
                & (df["Creation Date"].dt.strftime("%Y-%m-%d")
                   <= month_end)
            ]
            dataframes["new_clients_monthly"] = df_filtered
            summary_data.append({
                "Record Type": "New Clients/Month",
                "Count": len(df_filtered)
            })

        if report_paths.get("benefits_and_applications"):
            df = pd.read_csv(
                report_paths["benefits_and_applications"]
            )
            df["Start Date"] = pd.to_datetime(
                df["Start Date"], errors="coerce"
            )
            df_filtered = df[
                (df["Start Date"].dt.strftime("%Y-%m-%d")
                 >= month_start)
                & (df["Start Date"].dt.strftime("%Y-%m-%d")
                   <= month_end)
            ]
            dataframes["benefits_and_applications"] = df_filtered
            summary_data.append({
                "Record Type": "Benefits and Applications",
                "Count": len(df_filtered)
            })

        if report_paths.get("housing_applications"):
            df = pd.read_csv(report_paths["housing_applications"])
            df["Date Submitted"] = pd.to_datetime(
                df["Date Submitted"], errors="coerce"
            )
            df_filtered = df[
                (df["Date Submitted"].dt.strftime("%Y-%m-%d")
                 >= month_start)
                & (df["Date Submitted"].dt.strftime("%Y-%m-%d")
                   <= month_end)
            ]
            dataframes["housing_applications"] = df_filtered
            summary_data.append({
                "Record Type": "Housing Applications",
                "Count": len(df_filtered)
            })

        summary_df = pd.DataFrame(summary_data)
        summary_df.insert(0, "Period", f"{month_start} to {month_end}")
        summary_file = "report_summary.csv"
        summary_df.to_csv(summary_file, index=False)

        for report_name, file_path in report_paths.items():
            try:
                Path(file_path).unlink()
                print(f"🗑️  Deleted: {file_path}")
            except Exception as e:
                print(f"⚠️  Could not delete {file_path}: {e}")

        return dataframes


def main():
    """Main entry point with CLI argument parsing."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Apricot Reports Manager - Download All Reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Login and download all 3 reports
  python apricot_reports.py

  # Debug with visible browser
  python apricot_reports.py --headed
        """,
    )

    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run in headed mode (visible browser) for debugging",
    )

    args = parser.parse_args()

    print("🔄 Starting Apricot Reports Manager...\n")
    if args.headed:
        print("👁️  Running in HEADED mode (browser visible)\n")
    else:
        print("🙈 Running in HEADLESS mode (browser hidden)\n")

    manager = ApricotReportManager()
    phpsessid, csrftoken = manager.login(headed=args.headed)

    if not phpsessid or not csrftoken:
        print("\n❌ Login failed. Aborting report downloads.")
        sys.exit(1)

    print("\n✅ Login successful! Starting report downloads...\n")

    reports = [
        ("new_clients_monthly", manager.fetch_new_clients_monthly),
        (
            "benefits_and_applications",
            manager.fetch_benefits_and_applications,
        ),
        ("housing_applications", manager.fetch_housing_applications),
    ]

    results = []
    report_paths = {}
    for report_name, fetch_func in reports:
        result = fetch_func()
        results.append((report_name, result))
        if result:
            report_paths[report_name] = result

    all_success = all(result for _, result in results)

    if all_success and report_paths:
        print("\n📊 Loading and filtering reports by previous month...\n")
        ApricotReportManager.load_and_filter_dataframes(report_paths)
        print("\n✅ All reports downloaded, filtered, and summary created!")
        sys.exit(0)
    else:
        print("\n⚠️  Some reports failed to download. Skipping filtering.")
        sys.exit(1 if not all_success else 0)


if __name__ == "__main__":
    main()
