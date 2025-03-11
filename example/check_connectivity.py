import getpass
import configparser
from pathlib import Path

from TM1py import TM1Service
from TM1py.Exceptions import TM1pyRestException

if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.read(Path(__file__).parent.joinpath('config_example.ini'))
    try:
        tm1 = TM1Service(**config['tm1srv_example'])

    except TM1pyRestException:
        user = input("TM1 User (leave empty if SSO): ")
        password = getpass.getpass("Password (leave empty if SSO): ")
        namespace = input("CAM Namespace (leave empty if no CAM Security): ")
        address = input("Address (leave empty if localhost): ") or "localhost"
        gateway = input("ClientCAMURI (leave empty if no SSO): ")
        port = input("HTTP Port (Default 5000): ") or "5000"
        ssl_str = input("SSL (Default T or F): ") or "T"
        ssl = False

        if len(namespace.strip()) == 0:
            namespace = None

        if len(gateway.strip()) == 0:
            gateway = None

        if ssl_str == 'T':
            ssl = True

        tm1 = TM1Service(
            address=address,
            port=port,
            user=user,
            password=password,
            namespace=namespace,
            gateway=gateway,
            ssl=ssl
        )

    try:
        server_name = tm1.server.get_server_name()
        print("Connection to TM1 established! Your server name is: {}".format(server_name))
    except Exception as e:
        print("\nERROR:")
        print("\t" + str(e))
    finally:
        tm1.logout()