import socket
import time

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(("localhost", 12345))
server_socket.listen(1)
print("Waiting for connection...")
socket_connection, address = server_socket.accept()
print("socket accepted")


def sendData(timestamp, data):
    message = f"{timestamp},{data}\n"
    socket_connection.send(message.encode())


# Function to send data for example 1
def example1():
    startTime = 0
    time.sleep(7)
    sendData(startTime + 7, "blue")
    time.sleep(1)
    sendData(startTime + 8, "green")
    time.sleep(4)
    sendData(startTime + 14, "blue")
    time.sleep(1)
    sendData(startTime + 9, "red")
    time.sleep(3)
    sendData(startTime + 15, "red")
    sendData(startTime + 8, "blue")
    time.sleep(1)
    sendData(startTime + 13, "green")
    time.sleep(0.5)
    sendData(startTime + 21, "green")  # dropped
    time.sleep(3)
    sendData(startTime + 4, "purple")  # Expected to be dropped - it's older than the watermark
    time.sleep(2)
    sendData(startTime + 17, "green")


# Function to send data for example 2
def example2():
    startTime = 0
    sendData(startTime + 5, "red")
    sendData(startTime + 5, "green")
    sendData(startTime + 4, "blue")
    time.sleep(7)
    sendData(startTime + 10, "yellow")
    sendData(startTime + 20, "cyan")
    sendData(startTime + 30, "magenta")
    sendData(startTime + 50, "black")
    time.sleep(3)
    sendData(startTime + 100, "pink")  # Dropped


# Function to send data for example 3
def example3():
    startTime = 0
    time.sleep(2)
    sendData(startTime + 9, "blue")
    time.sleep(3)
    sendData(startTime + 2, "green")
    sendData(startTime + 1, "blue")
    sendData(startTime + 8, "red")
    time.sleep(2)
    sendData(startTime + 5, "red")
    sendData(startTime + 18, "blue")
    time.sleep(1)
    sendData(startTime + 2, "green")
    time.sleep(2)
    sendData(startTime + 30, "purple")  # Discarded
    sendData(startTime + 10, "green")


if __name__ == "__main__":
    example1()
