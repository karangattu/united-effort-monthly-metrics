import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv


class AirtableManager:
    """Manages fetching and filtering data from Airtable."""

    def __init__(self):
        """Initialize the Airtable manager with API token."""
        load_dotenv()
        self.token = os.getenv("AIRTABLE_TOKEN")
        if not self.token:
            raise ValueError("❌ AIRTABLE_TOKEN not found in .env file")
        
        self.base_id = "appqOF4YYlalhY8so"
        self.table_id = "tbljwCFQOomjEVWB8"
        self.base_url = (
            f"https://api.airtable.com/v0/"
            f"{self.base_id}/{self.table_id}"
        )
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def fetch_records(self):
        """
        Fetch all records from Airtable.

        Returns:
            list: List of records from Airtable
        """
        try:
            all_records = []
            offset = None

            while True:
                url = self.base_url
                params = {}
                if offset:
                    params["offset"] = offset

                response = requests.get(
                    url, headers=self.headers, params=params
                )
                response.raise_for_status()

                data = response.json()
                all_records.extend(data.get("records", []))

                offset = data.get("offset")
                if not offset:
                    break

            print(f"✅ Fetched {len(all_records)} records from Airtable")
            return all_records

        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching from Airtable: {e}")
            return None

    @staticmethod
    def records_to_dataframe(records):
        """
        Convert Airtable records to pandas DataFrame.

        Args:
            records (list): List of Airtable records

        Returns:
            pd.DataFrame: DataFrame with record fields
        """
        if not records:
            return pd.DataFrame()

        data = []
        for record in records:
            fields = record.get("fields", {})
            fields["id"] = record.get("id")
            data.append(fields)

        return pd.DataFrame(data)

    @staticmethod
    def filter_by_previous_month(df):
        """
        Filter dataframe to records with Start Date in previous month.

        Args:
            df (pd.DataFrame): Input dataframe

        Returns:
            pd.DataFrame: Filtered dataframe
        """
        date_column = "Start Date"
        if date_column not in df.columns:
            print(f"⚠️  Column '{date_column}' not found in dataframe")
            return df

        today = datetime.now()
        first_of_current = today.replace(day=1)
        last_of_previous = first_of_current - timedelta(days=1)
        first_of_previous = last_of_previous.replace(day=1)

        month_start = first_of_previous
        month_end = last_of_previous

        df[date_column] = pd.to_datetime(
            df[date_column], format="%Y-%m-%d", errors="coerce"
        )

        df_filtered = df[
            (df[date_column] >= month_start)
            & (df[date_column] <= month_end)
        ].copy()

        month_str = month_start.strftime("%Y-%m-%d")
        month_end_str = month_end.strftime("%Y-%m-%d")
        print(
            f"✅ Filtered to {len(df_filtered)} records "
            f"with Start date between {month_str} and {month_end_str}"
        )

        return df_filtered

    def save_to_csv(self, df, filename):
        """
        Save dataframe to CSV file.

        Args:
            df (pd.DataFrame): Dataframe to save
            filename (str): Output filename

        Returns:
            str: Path to saved file
        """
        try:
            output_path = Path(filename)
            df.to_csv(output_path, index=False)
            print(f"✅ Saved to: {output_path.absolute()}")
            return str(output_path.absolute())
        except Exception as e:
            print(f"❌ Error saving to CSV: {e}")
            return None

    def fetch_event_attendance(self):
        """
        Fetch all attendance records from the Event Attendance table.

        Returns:
            list: List of attendance records from Airtable
        """
        try:
            all_records = []
            offset = None
            event_table_id = "tbl7ePbU3BVJK9x0l"
            event_url = (
                f"https://api.airtable.com/v0/"
                f"{self.base_id}/{event_table_id}"
            )

            while True:
                params = {}
                if offset:
                    params["offset"] = offset

                response = requests.get(
                    event_url, headers=self.headers, params=params
                )
                response.raise_for_status()

                data = response.json()
                all_records.extend(data.get("records", []))

                offset = data.get("offset")
                if not offset:
                    break

            print(
                f"✅ Fetched {len(all_records)} attendance records "
                f"from Event Attendance table"
            )
            return all_records

        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching from Airtable: {e}")
            return None

    @staticmethod
    def calculate_volunteer_return_rate(records):
        """
        Calculate volunteer return rate from event attendance records.

        A returning volunteer is one who appears at least twice within
        a 6-month window.

        Args:
            records (list): List of attendance records

        Returns:
            tuple: (return_rate_percentage, period_start, period_end)
        """
        if not records:
            return 0.0, None, None

        data = []
        for record in records:
            fields = record.get("fields", {})
            data.append(fields)

        df = pd.DataFrame(data)

        if df.empty or "Name" not in df.columns or \
           "Event Date" not in df.columns:
            print("⚠️  Required columns not found in attendance data")
            return 0.0, None, None

        today = datetime.now()
        first_of_current = today.replace(day=1)
        last_of_previous = first_of_current - timedelta(days=1)
        first_of_previous = last_of_previous.replace(day=1)
        six_months_ago = first_of_previous - timedelta(days=180)

        period_start = six_months_ago.strftime("%Y-%m-%d")
        period_end = last_of_previous.strftime("%Y-%m-%d")

        # Handle Event Date as lookup field (list values)
        def extract_first_date(date_val):
            """Extract first date from list or return string."""
            if isinstance(date_val, list):
                return date_val[0] if date_val else None
            return date_val

        df["Event Date"] = df["Event Date"].apply(extract_first_date)

        df["Event Date"] = pd.to_datetime(
            df["Event Date"], errors="coerce"
        )

        df_filtered = df[
            (df["Event Date"] >= six_months_ago)
            & (df["Event Date"] <= last_of_previous)
        ].copy()

        if df_filtered.empty:
            print(
                f"⚠️  No attendance records found between "
                f"{period_start} and {period_end}"
            )
            return 0.0, period_start, period_end

        volunteer_appearance_count = (
            df_filtered.groupby("Name").size().reset_index(name="count")
        )

        total_unique_volunteers = len(volunteer_appearance_count)
        returning_volunteers = len(
            volunteer_appearance_count[
                volunteer_appearance_count["count"] >= 2
            ]
        )

        return_rate = (
            (returning_volunteers / total_unique_volunteers * 100)
            if total_unique_volunteers > 0 else 0.0
        )

        print(
            f"✅ Volunteer Return Rate Analysis "
            f"({period_start} to {period_end}):"
        )
        print(f"   Total unique volunteers: {total_unique_volunteers}")
        print(f"   Returning volunteers (2+ events): {returning_volunteers}")
        print(f"   Return rate: {return_rate:.2f}%")

        return return_rate, period_start, period_end

    @staticmethod
    def count_unique_volunteers(records):
        """
        Count unique volunteers from event attendance in previous month.

        Args:
            records (list): List of attendance records

        Returns:
            tuple: (unique_volunteer_count, period_start, period_end)
        """
        if not records:
            return 0, None, None

        data = []
        for record in records:
            fields = record.get("fields", {})
            data.append(fields)

        df = pd.DataFrame(data)

        if df.empty or "Name" not in df.columns or \
           "Event Date" not in df.columns:
            print("⚠️  Required columns not found in attendance data")
            return 0, None, None

        today = datetime.now()
        first_of_current = today.replace(day=1)
        last_of_previous = first_of_current - timedelta(days=1)
        first_of_previous = last_of_previous.replace(day=1)

        period_start = first_of_previous.strftime("%Y-%m-%d")
        period_end = last_of_previous.strftime("%Y-%m-%d")

        def extract_first_date(date_val):
            """Extract first date from list or return string."""
            if isinstance(date_val, list):
                return date_val[0] if date_val else None
            return date_val

        df["Event Date"] = df["Event Date"].apply(extract_first_date)

        df["Event Date"] = pd.to_datetime(
            df["Event Date"], errors="coerce"
        )

        df_filtered = df[
            (df["Event Date"] >= first_of_previous)
            & (df["Event Date"] <= last_of_previous)
        ].copy()

        if df_filtered.empty:
            print(
                f"⚠️  No attendance records found between "
                f"{period_start} and {period_end}"
            )
            return 0, period_start, period_end

        unique_volunteers = df_filtered["Name"].nunique()

        print(
            f"✅ Unique Volunteers in {period_start} to {period_end}: "
            f"{unique_volunteers}"
        )

        return unique_volunteers, period_start, period_end

    @staticmethod
    def append_to_summary(record_count, return_rate=None,
                          return_rate_period=None, unique_volunteers=None,
                          unique_volunteers_period=None):
        """
        Append filtered Airtable data counts to report_summary.csv.

        Args:
            record_count (int): Number of filtered new volunteer records
            return_rate (float): Volunteer return rate percentage (optional)
            return_rate_period (tuple): (period_start, period_end) tuple
                                       (optional)
            unique_volunteers (int): Count of unique volunteers (optional)
            unique_volunteers_period (tuple): (period_start, period_end)
                                              tuple (optional)

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            today = datetime.now()
            first_of_current = today.replace(day=1)
            last_of_previous = first_of_current - timedelta(days=1)
            first_of_previous = last_of_previous.replace(day=1)

            month_start = first_of_previous.strftime("%Y-%m-%d")
            month_end = last_of_previous.strftime("%Y-%m-%d")
            period = f"{month_start} to {month_end}"

            summary_path = Path("report_summary.csv")
            entries = [
                {
                    "Period": period,
                    "Record Type": "New Volunteers",
                    "Count": record_count
                }
            ]

            if unique_volunteers is not None and unique_volunteers_period:
                period_start, period_end = unique_volunteers_period
                unique_volunteers_period_str = (
                    f"{period_start} to {period_end}"
                )
                entries.append({
                    "Period": unique_volunteers_period_str,
                    "Record Type": "Total Volunteers",
                    "Count": unique_volunteers
                })

            if return_rate is not None and return_rate_period:
                period_start, period_end = return_rate_period
                return_rate_period_str = (
                    f"{period_start} to {period_end}"
                )
                entries.append({
                    "Period": return_rate_period_str,
                    "Record Type": "Volunteer return rate",
                    "Count": f"{return_rate:.2f}%"
                })

            entry_df = pd.DataFrame(entries)

            if summary_path.exists():
                existing_df = pd.read_csv(summary_path)
                updated_df = pd.concat(
                    [existing_df, entry_df], ignore_index=True
                )
                updated_df.to_csv(summary_path, index=False)
                print(f"✅ Appended to: {summary_path.absolute()}")
            else:
                entry_df.to_csv(summary_path, index=False)
                print(f"✅ Created: {summary_path.absolute()}")

            airtable_data_path = Path("airtable_data.csv")
            if airtable_data_path.exists():
                airtable_data_path.unlink()

            return True
        except Exception as e:
            print(f"❌ Error updating summary: {e}")
            return False


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Airtable Data Fetcher - Filter by previous month",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch and filter by Start date
  python airtable_fetch.py
        """,
    )

    parser.parse_args()

    print("🔄 Starting Airtable Data Fetcher...\n")

    try:
        manager = AirtableManager()
        print("✅ Airtable manager initialized\n")

        records = manager.fetch_records()
        if not records:
            print("\n❌ No records fetched. Aborting.")
            return 1

        df = manager.records_to_dataframe(records)
        print(
            f"📊 Dataframe created with {len(df)} rows "
            f"and {len(df.columns)} columns\n"
        )

        df_filtered = manager.filter_by_previous_month(df)

        if len(df_filtered) == 0:
            print("⚠️  No records found in previous month")
        else:
            manager.save_to_csv(df_filtered, "airtable_data.csv")

        attendance_records = manager.fetch_event_attendance()
        return_rate = None
        return_rate_period = None
        unique_volunteers = None
        unique_volunteers_period = None

        if attendance_records:
            return_rate, period_start, period_end = (
                manager.calculate_volunteer_return_rate(
                    attendance_records
                )
            )
            return_rate_period = (period_start, period_end)
            
            unique_volunteers, u_period_start, u_period_end = (
                manager.count_unique_volunteers(attendance_records)
            )
            unique_volunteers_period = (u_period_start, u_period_end)
            print()

        manager.append_to_summary(
            len(df_filtered),
            return_rate=return_rate,
            return_rate_period=return_rate_period,
            unique_volunteers=unique_volunteers,
            unique_volunteers_period=unique_volunteers_period
        )
        return 0

    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
