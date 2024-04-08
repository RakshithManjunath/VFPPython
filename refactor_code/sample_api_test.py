from fastapi import FastAPI
import pandas as pd
import uvicorn

app = FastAPI()

@app.get("/get_wdtest_csv")
async def read_data():
    # Load the CSV data into a DataFrame
    df = pd.read_csv("wdtest.csv")
    # Convert the DataFrame to a JSON string and then parse it into a dictionary
    return df.to_dict(orient="records")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9876, reload=False)