import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import os
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

    @staticmethod
    def append_to_summary(record_count):
        """
        Append filtered Airtable record count to report_summary.csv.

        Args:
            record_count (int): Number of filtered records

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
            entry = pd.DataFrame([{
                "Period": period,
                "Record Type": "New Volunteers",
                "Count": record_count
            }])

            if summary_path.exists():
                existing_df = pd.read_csv(summary_path)
                updated_df = pd.concat(
                    [existing_df, entry], ignore_index=True
                )
                updated_df.to_csv(summary_path, index=False)
                print(f"✅ Appended to: {summary_path.absolute()}")
            else:
                entry.to_csv(summary_path, index=False)
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

        manager.append_to_summary(len(df_filtered))
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
