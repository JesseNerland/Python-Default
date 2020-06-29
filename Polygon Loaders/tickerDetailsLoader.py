import requests
import datetime
from polygon import RESTClient
import mysql.connector as mysql

# Take keys from API response, remove unnecessary data and flatten out nested "code" identifier keys
def cleanTickerKeys(tickerResponse):
    keys = list(tickerResponse.keys())
    keys.remove('url')

    if 'codes' in keys:
        codes = list(tickerResponse['codes'].keys())

        for code in codes:
            keys.append(code)

        keys.remove('codes')
    
    return keys

# Get headers from first entry in API response
def getHeaders(client):
    resp = client.reference_tickers(page=1,perpage=1)
    headers = cleanTickerKeys(resp.tickers[0])

    return headers

# Generate MySQL insert statement given column names and values
def tickerInsertStatement(hdrs,vals):
    headers = hdrs.copy()
    headers[headers.index('primaryExch')] = 'primary_exchange'
    headers[headers.index('updated')] = 'api_updated'
    headers = ", ".join(headers)

    # Join all data values to single string 
    # if any value contains a single quote (') then wrap value with double quote
    # if any value contains a double quote (") or is a boolean, wrap value with single quotes
    values = ", ".join('"{0}"'.format(v) if v != 'NULL' and type(v) != bool and '"' not in v else '{0}'.format(v) for v in vals)

    insert = "INSERT INTO ticker ({}) VALUES ({})".format(headers, values)
    
    return insert

# Main Function
def main():

    # Connect to database
    db = mysql.connect(host = "localhost", user = "root", passwd = "Jn2790472015$", database = "data")
    cursor = db.cursor()

    # API Key and client connection
    key = "d25I_1rdsjEHIan3ZoLXiGv_3mfcLY0wRkDpv9"
    client = RESTClient(key)

    # Get  headers and declare list of unique identifiers found in polygon ticker data
    headers = getHeaders(client)
    potential_codes = ['cik', 'figiuid', 'scfigi', 'cfigi', 'figi']
    
    # Declare page - set to 1 to start loop on first page (50 items max per page as per Polygon documentation)
    page = 1
    
    # Continue to generate API requests with increasing page number while length of the API response remains non-zero 
    while (page == 1 or len(resp.tickers) > 0):
        resp = client.reference_tickers(page=page)
        page += 1

        # For page of results clean the data and serialize to list for insert statement
        for result in resp.tickers:
            data_line = []
            keys = cleanTickerKeys(result)
            
            # Lining up JSON values with serialized headers
            for header in headers:
                if header in keys:
                    if header in potential_codes:
                        data_line.append(result['codes'][header])
                    else:
                        data_line.append(result[header])
                else:
                    data_line.append('NULL')
            
            # Attempt to run MySQL insert statement, if it fails skip and continue
            try:
                cursor.execute(tickerInsertStatement(headers,data_line))
                db.commit()
                print(f'{data_line[1]} ({data_line[0]}): Successfully inserted')
            except:
                print('SQL Error')
                pass
            
if __name__ == '__main__':
    main()
    cursor.close()
