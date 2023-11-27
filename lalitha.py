from datetime import datetime
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import yaml

CFG_NAME = "cfg.yml"
SCOPES = ["https://www.googleapis.com/auth/calendar"]
DEF_PATH = os.environ.get("LALITHA_PATH")


def get_sheets(cfg):
    """Get sheets from excel file in pandas dataframe format"""
    df = pd.read_excel(
        os.path.join(DEF_PATH, cfg["file_name"]), sheet_name=cfg["sheets"]
    )
    return df


def get_event_from_entry(cfg, date, role):
    """Translate an entry to a dict with values to send to google calendar"""
    # Get the title of the event
    role_info = cfg["role_info"][role]
    title, location = role_info["title"], role_info["location"]

    # Get the description of the event
    # Will look like: "Last updated by Lalitha on 05/28/2021 at 09:00 AM"
    curr_time = pd.Timestamp.now().strftime("%m/%d/%Y at %I:%M %p")
    description = f"Last updated by Lalitha on {curr_time}\n"

    # Start and end time is fetched from the config and depends on the role
    # It is in HH:MM AM/PM format, convert to google calendar format
    start_time = pd.to_datetime(role_info["start_time"], format="%I:%M %p").time()
    end_time = pd.to_datetime(role_info["end_time"], format="%I:%M %p").time()

    # Combine the date and time to get the start and end datetimes
    # Format is '2022-05-28T09:00:00'
    start_datetime = datetime.combine(date, start_time).strftime("%Y-%m-%dT%H:%M:%S")
    end_datetime = datetime.combine(date, end_time).strftime("%Y-%m-%dT%H:%M:%S")

    return dict(
        title=title,
        location=location,
        description=description,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
    )


def get_events_from_df(cfg, df_dict):
    """Get events from dataframe. This is an iterator to make it efficient."""

    for _sheet_name, df in df_dict.items():
        # Iterate over these results
        scheduled = df.isin(cfg["aliases"]).stack()
        for indices, value in scheduled.items():
            if not value:
                continue
            
            # Unpack indices
            row_i, col_i = indices

            # First row is the role you are performing
            role = df.iloc[row_i, 0]

            # Format of the date is "mm/dd/yyyy"
            # The first cell above the current cell having text in date format
            # is the date
            for above_row in range(row_i - 1, -1, -1):
                if pd.isna(df[col_i][above_row]):
                    continue
                try:
                    date = pd.to_datetime(
                        df[col_i][above_row], format="%m/%d/%Y"
                    ).date()
                    date_row = above_row
                    break
                except ValueError:
                    continue
            else:
                raise ValueError(f"No date was found for entry in coordinates {indices}")

            # Sanity check: The day of the week should match the day of the date
            # The cell above the date cell found before should be the day of the week
            day_of_week = df[col_i][date_row-1]
            assert (
                date.strftime("%A") == day_of_week
            ), f"Day of week does not match date for entry in coordinates {indices}"

            # Everything looks good, create the event for this entry
            yield get_event_from_entry(cfg, date, role)


def get_cfg():
    # Import config from a YAML file
    cfg = yaml.safe_load(open(os.path.join(DEF_PATH, CFG_NAME)))

    # Add some variables based on the cfg
    cfg["token_path"] = os.path.join(DEF_PATH, cfg["token_name"])
    cfg["cred_path"] = os.path.join(DEF_PATH, cfg["cred_name"])
    return cfg


def login(cfg):
    """Login to google calendar"""
    creds = None
    # The file token_path stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(cfg["token_path"]):
        creds = Credentials.from_authorized_user_file(cfg["token_path"], SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(cfg["cred_path"], SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(cfg["token_path"], "w") as token:
            token.write(creds.to_json())

    service = build("calendar", "v3", credentials=creds)
    return service


def get_calendar(cfg, service):
    """Check if calendar exists, if not create it. Returns calendar id."""
    # Call the Calendar API
    print("Getting list of calendars")

    calendars_result = service.calendarList().list().execute()
    calendars = calendars_result.get("items", [])

    if not calendars:
        print("No calendars found.")
    for calendar in calendars:
        summary, id = calendar["summary"], calendar["id"]

        if summary == cfg["calendar_name"]:
            print(f"Found calendar: {summary} - {id}")
            break
    else:
        # Create a new calendar
        print(f"Calendar not found, creating new calendar: {cfg['calendar_name']}")

        # Create the calendar with user's timezone
        calendar = {"summary": cfg["calendar_name"], "timeZone": cfg["timezone"]}

        created_calendar = service.calendars().insert(body=calendar).execute()
        summary, id = created_calendar["summary"], created_calendar["id"]
        print(f"Created calendar: {summary} - {id}")
    return id


def create_event(cfg, service, batch, calendar_id, event, recurrence=None):
    body = {
        "creator": {"displayName": cfg["creator_name"]},
        "source": {"title": cfg["source"]['title'], "url": cfg["source"]['url']},
        "summary": event["title"],
        "location": event["location"],
        "description": event["description"],
        "start": {
            "dateTime": event["start_datetime"],
            "timeZone": cfg["timezone"],
        },
        "end": {
            "dateTime": event["end_datetime"],
            "timeZone": cfg["timezone"],
        },
    }

    if "recurrence" in event:
        body["recurrence"] = event["recurrence"]

    batch.add(service.events().insert(calendarId=calendar_id, body=body))


def get_recurrence_events(cfg):
    for entry in cfg["recurring_schedules"]:
        # Get the title of the event
        title, location, days, start_time, end_time, start_date, end_date = (
            entry["title"],
            entry["location"],
            entry["days"],
            entry["start_time"],
            entry["end_time"],
            entry["start_date"],
            entry["end_date"],
        )

        # Get the description of the event
        # Will look like: "Last updated by Lalitha on 05/28/2021 at 09:00 AM"
        curr_time = pd.Timestamp.now().strftime("%m/%d/%Y at %I:%M %p")
        description = f"Last updated by Lalitha on {curr_time}\n"

        # Start and end time is fetched from the config and depends on the role
        # It is in HH:MM AM/PM format, convert to google calendar format
        start_time = pd.to_datetime(start_time, format="%I:%M %p").time()
        end_time = pd.to_datetime(end_time, format="%I:%M %p").time()

        # Date is little tricky; it needs to be the first date when this rule is satisfied
        # So if the start date is a Tuesday and the days are Monday, Wednesday, Friday then the first date is Wednesday
        first_date = pd.to_datetime(start_date, format="%m/%d/%Y")
        while first_date.strftime("%A") not in days:
            first_date += pd.DateOffset(days=1)
        date = first_date.date()

        # The date when the recurrence should end in the format '20221231'
        end_date = pd.to_datetime(end_date, format="%m/%d/%Y").strftime("%Y%m%d")

        # Combine the date and time to get the start and end datetimes
        # Format is '2022-05-28T09:00:00'
        start_datetime = datetime.combine(date, start_time).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        end_datetime = datetime.combine(date, end_time).strftime("%Y-%m-%dT%H:%M:%S")

        # 'days' key has the recurrence rule but full names of days; convert to RRULE format
        rrule_days = (x[:2].upper() for x in days)
        recurrence = f"RRULE:FREQ=WEEKLY;BYDAY={','.join(rrule_days)};UNTIL={end_date}"

        yield dict(
            title=title,
            location=location,
            description=description,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            recurrence=recurrence,
        )


def main():
    cfg = get_cfg()

    # Calls to parse the excel file and get the schedule
    sheets = get_sheets(cfg)

    # The calendar API calls
    service = login(cfg)
    calendar_id = get_calendar(cfg, service)

    # Create a batch request
    batch = service.new_batch_http_request()

    # First create events for schedule in the excel file
    for event in get_events_from_df(cfg, sheets):
        create_event(cfg, service, batch, calendar_id, event)

    # Next create events for recurring schedules in the config
    for event in get_recurrence_events(cfg):
        create_event(cfg, service, batch, calendar_id, event)

    # Execute the batch request
    batch.execute()


if __name__ == "__main__":
    try:
        main()
    except HttpError as error:
        print(f"An error occurred: {error}")
