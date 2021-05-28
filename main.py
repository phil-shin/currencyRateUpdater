import pyodbc 
import requests
import json
import time

def main():
    '''
    By: Phil Shin

    Reworks To Add for Performance Improvements: 
        - Query for distinct ToCurrencyCode values and store
        - Loop through currency codes to call API once per distinct currency code and store conversion rates (in code-rate key-value dict)
        - Loop through conversion rates and do a bulk update per currency code (where clause based on ToCurrencyCode)
        * Above improvements will cut down on total iterations & reduce API calls -> But overall time complexity should be O(n) regardless
    '''
    
    while True:
        '''Connect to local sql container'''
        conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                            'Server=0.0.0.0,1433;'
                            'Database=AdventureWorks2019;'
                            'UID=sa;'
                            'PWD=Simple123;')

        '''Select query to pull all Currency Rate Rows'''
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM AdventureWorks2019.Sales.CurrencyRate')

        for row in cursor.fetchall():
            '''pull out essential values from the row'''
            id = row.CurrencyRateID
            currencyCode = row.ToCurrencyCode

            '''Call currency rate API to find latest conversion rate'''
            url = f'https://free.currconv.com/api/v7/convert?q=USD_{currencyCode}&compact=ultra&apiKey=462996df385e07eeb77a'
            response = requests.get(url)
            res = response.json()

            '''Error Handle if currency code is not supported by API'''
            if len(res) < 1:
                continue

            '''Error Handle if API limit is reached'''
            if 'error' in res.keys():
                print('API limit reached!')
                break

            '''Update current row in database with new currency rate'''
            latestRate = res['USD_'+currencyCode]
            query = f'''
            UPDATE AdventureWorks2019.Sales.CurrencyRate
            SET EndOfDayRate = {latestRate},
                CurrencyRateDate = CURRENT_TIMESTAMP,
                ModifiedDate = CURRENT_TIMESTAMP
            WHERE CurrencyRateID = {id}
            '''
            cursor.execute(query)
            conn.commit()

            print(f'Updated id: {id}')

        conn.close()

        '''Set 1 hour delay prior to all conversions being updating'''
        time.sleep(3600)

if __name__ == "__main__":
    main()