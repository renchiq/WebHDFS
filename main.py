import json
import sys
import os
import requests


class ClientWebHDFS:
    def __init__(self, server, port, user):
        self.server = server
        self.port = port
        self.user = user

        self.base_url = 'http://{}:{}/webhdfs/v1/'.format(server, port)

        self.current_url = ''

    def mkdir(self, directory):
        params = {
            'user.name': self.user,
            'op': 'MKDIRS'
        }
        request = requests.put(
            self.base_url + self.current_url + directory,
            params=params
        )
        response = request.text
        print('Response: {}'.format(response))

    def put(self, file_name):
        params = {
            'user.name': self.user,
            'op': 'CREATE',
            'overwrite': 'true'
        }
        request = requests.put(
            self.base_url + self.current_url + file_name,
            params=params
        )
        datanode_url = request.url

        file = open(file_name, 'rb')
        data = file.read(os.path.getsize(file_name))

        request = requests.put(
            datanode_url,
            data=data
        )

        response = request.text
        print('Response: {}'.format(response))

    def get(self, file_name):
        params = {
            'user.name': self.user,
            'op': 'OPEN'
        }
        request = requests.get(
            self.base_url + self.current_url + file_name,
            params=params
        )
        datanode_url = request.url
        request = requests.get(datanode_url)

        response = request.text
        print('Response: {}'.format(response))

    def append(self, local_file, hdfs_file):
        params = {
            'user.name': self.user,
            'op': 'APPEND'
        }
        request = requests.post(
            self.base_url + self.current_url + hdfs_file,
            params=params
        )
        datanode_url = request.url

        file = open(local_file, 'rb')
        data = file.read(os.path.getsize(local_file))

        request = requests.post(
            datanode_url,
            data=data
        )

        response = request.text
        print('Response: {}'.format(response))

    def delete(self, file_name):
        params = {
            'user.name': self.user,
            'op': 'DELETE'
        }
        request = requests.delete(
            self.base_url + self.current_url + file_name,
            params=params
        )

        response = request.text
        print('Response: {}'.format(response))

    def ls(self):
        params = {
            'user.name': self.user,
            'op': 'LISTSTATUS'
        }
        request = requests.get(
            self.base_url + self.current_url,
            params=params
        )

        response = json.loads(request.text)
        dirs_data = response['FileStatuses']['FileStatus']

        for elem in dirs_data:
            if elem['type'] == 'FILE':
                print('\033[36m{} '.format(elem['pathSuffix']))
            if elem['type'] == 'DIRECTORY':
                print('\033[34m{} '.format(elem['pathSuffix']))
        print('\033[0m', end='')

    def cd(self, path):
        if path == '..':
            if self.current_url != '':
                url_arr = self.current_url.split('/')
                url_arr.pop()
                self.current_url = '/'.join(url_arr)
        elif path.startswith('./'):
            if self.current_url != '':
                self.current_url += '{}/'.format(path[2:])
            else:
                self.current_url += '{}/'.format(path[2:])
        else:
            self.current_url = path
            if not self.current_url.endswith('/'):
                self.current_url += '/'
        # if not self.current_url.endswith('/') and not self.current_url == '':
        #     self.current_url += '/'

    def lls(self):
        dir_info = os.listdir(os.getcwd())

        for elem in dir_info:
            if os.path.isfile(elem):
                print('\033[36m{} '.format(elem))
            if os.path.isdir(elem):
                print('\033[34m{} '.format(elem))
        print('\033[0m', end='')

    def lcd(self, path):
        try:
            os.chdir(command[1])
        except FileNotFoundError:
            print(f"> Error: {sys.exc_info()}")


if __name__ == '__main__':
    arguments = sys.argv
    if len(arguments) != 4:
        print('To start work with HDFS client you need to specify 3 arguments\n'
              '1. Server (e.x. localhost)\n'
              '2. Port (e.x. 50070)\n'
              '3. User name (current system user)\n')
    else:
        server_arg = arguments[1]
        port_arg = arguments[2]
        user_arg = arguments[3]
        client = ClientWebHDFS(server_arg, port_arg, user_arg)
        print('Connection established. Enter command: ')
        while True:
            print('[/{}]'.format(client.current_url), end=' ')
            command = input()

            command = command.split(' ')

            if command[0] == 'exit':
                print('Client work was stopped.')
                break
            elif command[0] == 'mkdir':
                client.mkdir(command[1])
            elif command[0] == 'put':
                client.put(command[1])
            elif command[0] == 'get':
                client.get(command[1])
            elif command[0] == 'append':
                client.append(command[1], command[2])
            elif command[0] == 'delete':
                client.delete(command[1])
            elif command[0] == 'ls':
                client.ls()
            elif command[0] == 'cd':
                client.cd(command[1])
            elif command[0] == 'lls':
                client.lls()
            else:
                print('Invalid input. Try again:')
