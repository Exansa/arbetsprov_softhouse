
from time import sleep
import pandas as pd
import random
from datetime import timedelta
from filelock import FileLock

#Initial values
companies = ['NCC', 'ABB', 'AddLife B', 'SSAB B', '8TRA', 'Volvo A', 'Volvo B', 'SSAB A', 'H&M', 'ABC', 'Tre', 'Telia']

opening = pd.Timestamp('09:00:00')
closing = pd.Timestamp('18:00:00')

timedelta_min = 0
timedelta_max = 10

kursdelta_min = -10
kursdelta_max = 9

new_kurs_min = 50
new_kurs_max = 150

aktier = pd.read_csv("aktier.csv", sep=';')
aktier['Date'] = pd.to_datetime(aktier['Date'])


last_timestamp = aktier['Date'].iloc[-1]

lock = FileLock("aktier.csv.lock")


while True:
    for i in range(5):
        last_timestamp += timedelta(seconds=random.randint(timedelta_min, timedelta_max))
        while not (opening.time() <= last_timestamp.time() <= closing.time()):
            # If outside market hours, adjust to next day's opening
            last_timestamp = pd.Timestamp(f"{last_timestamp.date() + timedelta(days=1)} {opening.time()}")

        current_company = random.choice(companies)
      

        
        if current_company in aktier['Kod'].values:
            #get the latest kurs value for current 'Kod'
            last_kurs = aktier[aktier['Kod'] == current_company]['Kurs'].iloc[-1]

            last_kurs = max(0, last_kurs + random.randint(kursdelta_min, kursdelta_max)) 
        else:
            last_kurs = random.randint(new_kurs_min, new_kurs_max)  
        
       

        # Add latest row to Aktier
        aktier.loc[len(aktier)] = [last_timestamp, current_company, last_kurs]

        # Optional: Display the last row added
        new_data = {
            'Date': last_timestamp,
            'Kod': current_company,
            'Kurs': last_kurs
        }
        print(f"New row added: {new_data}")
        

        sleep(1)
        with lock:
            aktier.to_csv("aktier.csv", index=False, sep=';')

