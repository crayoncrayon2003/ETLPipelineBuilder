import requests

def main(endpoint,username, password):
    try:
        response = requests.get(endpoint, auth=(username, password))
        print(f"response code: {response.status_code}")
        print("response header:")
        for header, value in response.headers.items():
            print(f"  {header}: {value}")

        print("response body:\n")
        print(response.text)

        response.raise_for_status()

        content_disposition = response.headers.get('Content-Disposition')
        if content_disposition and 'filename=' in content_disposition:
            filename_start = content_disposition.find('filename="') + len('filename="')
            filename_end = content_disposition.find('"', filename_start)
            filename = content_disposition[filename_start:filename_end]

            with open(filename, 'w', encoding='utf-8') as f:
                f.write(response.text)
            print(f"\nCSVデータを '{filename}' として保存しました。")

    except requests.exceptions.HTTPError as e:
        print(f"HTTPエラーが発生しました: {e}")
        print(f"サーバーからのメッセージ:\n{e.response.text}")
    except requests.exceptions.ConnectionError as e:
        print(f"接続エラーが発生しました: サーバーが起動していないか、URLが間違っています。\n{e}")
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")

if __name__ == "__main__":
    SERVER_URL = "http://localhost:8080/device_data.csv"

    main(SERVER_URL,"testuser","local_secret_password_1234")
    main(SERVER_URL,"testuser","local_secret_password_123")