from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import sys
import pandas as pd
import os

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/calendar']


def generate_event(participants, schedule):

    player_a = schedule['player_a']
    a_email = next((item['email'] for item in participants if item["name"].lower() == player_a.lower()), None)
    player_b = schedule['player_b']
    b_email = next((item['email'] for item in participants if item["name"].lower() == player_b.lower()), None)

    if not a_email:
        raise KeyError("%s has not been registered!" % player_a)
    if not b_email:
        raise KeyError("%s has not been registered!" % player_b)

    summary = player_a + " vs " + player_b
    location = "Court 6"
    time_zone = "America/Edmonton"

    event = {
      'summary': summary,
      'location': location,
      'description': summary,
      'start': {
        'dateTime': schedule['date'] + "T" + schedule['start_time'],
        'timeZone': time_zone,
      },
      'end': {
        'dateTime': schedule['date'] + "T" + schedule['end_time'],
        'timeZone': time_zone,
      },
      'attendees': [
        {'email':  a_email},
        {'email':  b_email},
      ],
      'reminders': {
        'useDefault': False,
        'overrides': [
          {'method': 'popup', 'minutes': 30}
        ],
      },
    }

    return event


def generate_events(participants, schedules):
    events = []
    for schedule in schedules:
        event = generate_event(participants, schedule)
        events.append(event)
    return events


def init_connection():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('calendar', 'v3', credentials=creds)
    return service


def parse_csv(f):
    if not f.endswith(".csv"):
        raise TypeError("%s is not a csv type")
    if not os.path.isfile(f):
        raise FileNotFoundError("%s does not exist!" % f)
    df = pd.read_csv(f)
    return df.to_dict(orient='records')

def get_id(file_name):
   with open(f) as calendar:
        id = calendar.readline()
    return id


def main():

    id = get_id('calendar.txt')
    participant_file = "participants.csv"
    schedule_file = "winter-2020-tier-1-schedule.csv"

    try:
        participants = parse_csv(participant_file)
        schedules = parse_csv(schedule_file)
        events = generate_events(participants, schedules)
        service = init_connection()
        for event in events:
            print(event)
            resp = service.events().insert(calendarId=id, body=event).execute()

            print('Event created: %s' % (resp.get('htmlLink')))
    except Exception as e:
        print(e)
        sys.exit()


main()
