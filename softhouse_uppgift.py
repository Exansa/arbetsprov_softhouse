#!/usr/bin/env python3
from flask import Flask, request, jsonify, abort
import pandas as pd
from filelock import FileLock

app = Flask(__name__)



def get_aktier():
    lock = FileLock("aktier.csv.lock")
    try:
        with lock:
            aktier = pd.read_csv("aktier.csv", sep=';')
    except FileNotFoundError:
        abort(500, description="Data not found. Please try again later.")

    try:
        aktier['Date'] = pd.to_datetime(aktier['Date'])
        aktier['Kod'] = aktier['Kod'].astype(str)
        aktier['Kurs'] = aktier['Kurs'].astype(int)
    except Exception:
        abort(500, description="Error processing data file")
    

    if aktier.empty:
        abort(404, description="No data available.")

    return aktier



@app.route('/winners', methods=['GET'])
def get_winners():
    try:
        number = int(request.args.get('number', 3))  # Convert to int, default to 3
    except ValueError:
        abort(400, description="Invalid input: 'number' must be an integer.") 
    
    if number <= 0:
        abort(400, description="Invalid input: 'number' must be a positive integer.") 

    aktier = get_aktier()

    # Get date from the last row (simulated current day)
    last_date = aktier['Date'].iloc[-1].date()  
    
    
    today_data = aktier[aktier['Date'].dt.date == last_date]
    if today_data.empty:
        abort(404, description="No data from today available.")

    # Create a frame with both first and last value as a column for each 'Kod'
    first_last_prices = today_data.groupby('Kod')['Kurs'].agg(['first', 'last'])

    # Add daily increase as a column
    first_last_prices['daily_increase'] = ((first_last_prices['last'] / first_last_prices['first']) - 1) * 100

    winners = first_last_prices.sort_values(by='daily_increase', ascending=False)
    
    top_winners = winners.head(number).reset_index()

    
    winners_list = []
    for index, row in top_winners.iterrows():
        winners_list.append({
            "rank": index + 1,
            "name": row["Kod"],
            "percent": round(row["daily_increase"], 2),
            "latest": row["last"]
        })
 
    return jsonify({"winners": winners_list})


#Example get request
@app.route('/top', methods=['GET'])
def get_top_companies():
    try:
        number = int(request.args.get('number', 3))  # Convert to int, default to 3
    except ValueError:
        abort(400, description="Invalid input: 'number' must be an integer.") 
    
    if number <= 0:
        abort(400, description="Invalid input: 'number' must be a positive integer.") 

    aktier = get_aktier()
    

    aktier_sorted = aktier.drop_duplicates(subset=['Kod'], keep='last').sort_values(by='Kurs', ascending=False)

    top  = aktier_sorted.head(number).reset_index()

    top_list = []
    for index, row in top.iterrows():
        top_list.append({
            "Rank": index + 1,
            "Kod": row["Kod"],
            "Kurs": row["Kurs"],
            "Latest": row["Date"]
        })
    return jsonify({"top": top_list})


if __name__ == '__main__':
    app.run(debug=True)