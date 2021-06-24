from algo_trader import *


class Jango(AlgoTrader):
    def __init__(self, real_invest):
        super().__init__(0)

        self.real_invest = real_invest

        self.run()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    real_invest = True
    window = Jango(real_invest)
    window.show()
    app.exec_()