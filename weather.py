from typing import Any
import httpx
import os
import pandas
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# Constants
load_dotenv()
KMA_API_KEY = os.getenv("KMA_API_KEY")

# Initialize FastMCP server
mcp = FastMCP("weather")


def get_stn() -> int:
    """
    KMA API에서 사용할 스테이션 코드 리스트를 가져오는 함수입니다.
    """
    STN_URL = "https://apihub.kma.go.kr/api/typ01/url/stn_inf.php"

    params = {
        "inf": "SFC",
        "authKey": KMA_API_KEY,
        }
    
    print("STN code loading...")
    
    response = httpx.get(STN_URL, params=params, timeout=10)
    response.encoding = 'euc-kr'
    text = response.text

    print("data parsing...")

    lines = text.splitlines()
    data_lines = []
    start = False
    for line in lines:
        if line.startswith("#START7777"):
            start = True
            continue
        if line.startswith("#7777END"):
            break
        if start and not line.strip().startswith("#"):
            data_lines.append(line.strip())

    stn_dict = {}
    parsed_rows = [line.split(maxsplit=14) for line in data_lines if line]
    columns = [
            "ID", "LON", "LAT", "STN_SP", "HT", "HT_PA", "HT_TA", "HT_WD", "HT_RN",
            "STN", "STN_KO", "STN_EN", "FCT_ID", "LAW_ID", "BASIN"
        ]
    df = pandas.DataFrame(parsed_rows, columns=columns)

    for i in range(len(df)):
        stn_dict[df["STN_KO"][i]] = int(df["ID"][i])

    return stn_dict


# Define the weather function
@mcp.tool("weather")
def weather(location: str) -> Any:
    """
    주어진 위치에 대한 날씨 정보를 가져오는 함수입니다. 기준 날짜는 오늘입니다.
    만약 주어진 위치에 대한 stn 코드가 없다면, 입력받은 위치와 가장 가까운 위치의 stn 코드를 가져오도록 다시 실행합니다.
    이때, stn 코드는 get_stn() 함수를 통해 가져옵니다.

    Args: location (str): 날씨 정보를 가져올 위치의 이름입니다.
    Returns: dict: 날씨 정보가 포함된 JSON 객체입니다.
    """
    # Get the station code for the given location
    try:
        stn_dict = get_stn()
        if stn_dict == -1:
            return {"error": "STN 코드를 불러오는 데 실패하였습니다."}
        stn_code = stn_dict[location]
    except KeyError:
        raise ValueError(f"'{location}'에 해당하는 STN 코드를 찾을 수 없습니다.")

    # Make a request to the KMA API
    params = {
        "type": "json",
        "stn": stn_code,
        "authkey": KMA_API_KEY,
    }

    KMA_URL = 'https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-aws2_min'

    response = httpx.get(KMA_URL, params=params, timeout=20)

    # Check if the request was successful
    if response.status_code == 200:
        return response.json()
    else:
        return {"error": "Failed to fetch weather data"}


if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')