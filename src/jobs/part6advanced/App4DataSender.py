import socket
import time

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(("localhost", 12347))
server_socket.listen(1)
print("Waiting for connection...")
socket_connection, address = server_socket.accept()
print("socket accepted")


def send_data(timestamp, data):
    message = f"{timestamp},{data}\n"
    socket_connection.send(message.encode())


# Function to send data for example 1
def example1():
    start_time = 0
    time.sleep(7)
    send_data(start_time + 7, "blue")
    time.sleep(1)
    send_data(start_time + 8, "green")
    time.sleep(4)
    send_data(start_time + 14, "blue")
    time.sleep(1)
    send_data(start_time + 9, "red")
    time.sleep(3)
    send_data(start_time + 15, "red")
    send_data(start_time + 8, "blue")
    time.sleep(1)
    send_data(start_time + 13, "green")
    time.sleep(0.5)
    send_data(start_time + 21, "green")  # dropped
    time.sleep(3)
    send_data(start_time + 4, "purple")  # Expected to be dropped - it's older than the watermark
    time.sleep(2)
    send_data(start_time + 17, "green")
    send_data(start_time + 120, "cyan")
    send_data(start_time + 170, "magenta")


# Function to send data for example 2
def example2():
    start_time = 0
    send_data(start_time + 5, "red")
    send_data(start_time + 5, "green")
    send_data(start_time + 4, "blue")
    time.sleep(7)
    send_data(start_time + 10, "yellow")
    send_data(start_time + 20, "cyan")
    send_data(start_time + 30, "magenta")
    send_data(start_time + 50, "black")
    time.sleep(3)
    send_data(start_time + 100, "pink")  # Dropped


# Function to send data for example 3
def example3():
    start_time = 0
    time.sleep(2)
    send_data(start_time + 9, "blue")
    time.sleep(3)
    send_data(start_time + 2, "green")
    send_data(start_time + 1, "blue")
    send_data(start_time + 8, "red")
    time.sleep(2)
    send_data(start_time + 5, "red")
    send_data(start_time + 18, "blue")
    time.sleep(1)
    send_data(start_time + 2, "green")
    time.sleep(2)
    send_data(start_time + 30, "purple")  # Discarded
    send_data(start_time + 10, "green")
    time.sleep(6)
    send_data(start_time + 120, "cyan")
    send_data(start_time + 150, "magenta")


if __name__ == "__main__":
    example1()
