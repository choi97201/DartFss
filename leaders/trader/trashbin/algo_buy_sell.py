from algo_trader import *

class Login(AlgoTrader):
    def __init__(self, real_invest):
        super().__init__(2)

        self.real_invest = real_invest

        self.run()

    def run(self):
        super().run()
        # Tr 요청
        self.request_event_loop = QEventLoop()

        # 실계좌 투자하는 경우
        if self.real_invest:
            # 잔고 조회
            self.request_opw00005()
        self.area2_set()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    real_invest = True
    window = Login(real_invest)
    window.show()
    app.exec_()