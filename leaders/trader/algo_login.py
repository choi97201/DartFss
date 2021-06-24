from algo_trader import *

class Login(AlgoTrader):
    def __init__(self, real_invest):
        super().__init__(0)

        self.real_invest = real_invest
        self.send_signal = False

        self.run()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    real_invest = False
    window = Login(real_invest)
    window.show()
    app.exec_()