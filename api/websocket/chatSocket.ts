import { BASE_URL } from "@/constants/url";
import { useAuthStore } from "@/store/authStore";

// http://lalaalal.com → ws://lalaalal.com (자동 변환)
const WS_URL = BASE_URL.replace(/^http(s)?/, "ws") + "/ws/chat";

/**
 * WebSocket 연결 상태
 * - DISCONNECTED: 연결 안 됨 (초기 상태)
 * - CONNECTING: 연결 시도 중
 * - CONNECTED: 연결 성공
 * - RECONNECTING: 연결 끊김 후 재시도 중
 * - ERROR: 최대 재시도 횟수 초과 or 인증 실패
 */
export type ConnectionStatus =
  | "DISCONNECTED"
  | "CONNECTING"
  | "CONNECTED"
  | "RECONNECTING"
  | "ERROR";

// 서버 → 클라이언트 메시지 타입 정의
type IncomingMessage =
  | { type: "INFO"; payload: { message: string } } // 연결 환영 메시지
  | { type: "ERROR"; payload: { reason: string; message: string } } // 에러 응답
  | { type: "MESSAGE"; payload: { sender_nickname: string; message: string } } // 채팅 메시지
  | { type: "READ"; payload: { reader_nickname: string } }; // 읽음 알림 (백엔드 추가 예정)

type MessageHandler = (msg: IncomingMessage) => void;
type StatusHandler = (status: ConnectionStatus) => void;
type ErrorHandler = (reason: string, message: string) => void;

/**
 * WebSocket 채팅 클라이언트 매니저 (싱글톤)
 *
 * 역할:
 * - WebSocket 연결/해제 관리
 * - 자동 재연결 (Exponential Backoff: 5s → 10s → 20s → 30s → 30s)
 * - 메시지 전송 (JOIN, MESSAGE, READ)
 * - 연결 상태 변화 콜백 제공
 */
class ChatSocketManager {
  private socket: WebSocket | null = null;
  private roomId: number | null = null;
  private messageHandler: MessageHandler | null = null;
  private statusHandler: StatusHandler | null = null;
  private errorHandler: ErrorHandler | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private isManualClose = false; // 사용자가 직접 disconnect() 호출 시 재연결 방지
  private status: ConnectionStatus = "DISCONNECTED";
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5; // 최대 재연결 시도 횟수

  // 상태 변경 + 콜백 호출
  private setStatus(status: ConnectionStatus) {
    this.status = status;
    this.statusHandler?.(status);
  }

  /**
   * WebSocket 연결 시작
   * @param roomId 참여할 채팅방 ID
   * @param onMessage 메시지 수신 콜백
   * @param onStatus 연결 상태 변화 콜백 (UI 배너 업데이트용)
   * @param onError 에러 수신 콜백 (토스트 표시용)
   */
  connect(
    roomId: number,
    onMessage: MessageHandler,
    onStatus?: StatusHandler,
    onError?: ErrorHandler,
  ) {
    // 이미 같은 방에 연결 중이면 핸들러만 교체하고 소켓 재연결 스킵
    if (this.socket?.readyState === WebSocket.OPEN && this.roomId === roomId) {
      this.messageHandler = onMessage;
      this.statusHandler = onStatus ?? null;
      this.errorHandler = onError ?? null;
      return;
    }

    this.disconnect();
    this.isManualClose = false;
    this.roomId = roomId;
    this.messageHandler = onMessage;
    this.statusHandler = onStatus ?? null;
    this.errorHandler = onError ?? null;

    const token = useAuthStore.getState().token;
    if (!token) {
      console.warn("[ChatSocket] 토큰 없음");
      this.setStatus("ERROR");
      return;
    }

    this.setStatus(this.reconnectAttempts > 0 ? "RECONNECTING" : "CONNECTING");

    // → ?token= 쿼리스트링으로 인증
    this.socket = new WebSocket(`${WS_URL}?token=${token}`);

    this.socket.onopen = () => {
      console.log("[ChatSocket] 연결 성공");
      this.reconnectAttempts = 0;
      this.setStatus("CONNECTED");
      // 연결 직후 채팅방 참여 메시지 전송
      this.send({ type: "JOIN", room_id: roomId });
    };

    this.socket.onmessage = (event) => {
      try {
        const data: IncomingMessage = JSON.parse(event.data);
        if (__DEV__) console.log("[ChatSocket] 수신:", data);

        if (data.type === "ERROR") {
          console.error(
            "[ChatSocket] 에러:",
            data.payload.reason,
            data.payload.message,
          );
          this.errorHandler?.(data.payload.reason, data.payload.message);
          return;
        }

        this.messageHandler?.(data);
      } catch (e) {
        console.error("[ChatSocket] 파싱 실패", e);
      }
    };

    this.socket.onclose = (event) => {
      console.log("[ChatSocket] 연결 종료", event.code);

      if (!this.isManualClose) {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
          console.warn("[ChatSocket] 재연결 최대 시도 횟수 초과");
          this.setStatus("ERROR");
          return;
        }

        this.reconnectAttempts++;
        this.setStatus("RECONNECTING");

        // Exponential Backoff: 재시도 횟수에 따라 대기 시간 증가 (최대 30초)
        const delay = Math.min(
          5000 * Math.pow(2, this.reconnectAttempts - 1),
          30000,
        );

        console.log(
          `[ChatSocket] ${delay}ms 후 재연결 시도 (${this.reconnectAttempts}/${this.maxReconnectAttempts})`,
        );

        this.reconnectTimer = setTimeout(() => {
          if (this.roomId && this.messageHandler) {
            this.connect(
              this.roomId,
              this.messageHandler,
              this.statusHandler ?? undefined,
              this.errorHandler ?? undefined,
            );
          }
        }, delay);
      } else {
        this.setStatus("DISCONNECTED");
      }
    };

    this.socket.onerror = (error) => {
      console.error("[ChatSocket] WebSocket 에러", error);
    };
  }

  /** 메시지 전송 (WebSocket) */
  sendMessage(roomId: number, message: string): boolean {
    if (this.socket?.readyState !== WebSocket.OPEN) {
      console.warn("[ChatSocket] 연결 안 됨 - 전송 실패");
      return false;
    }
    this.send({ type: "MESSAGE", room_id: roomId, message });
    return true;
  }

  /**
   * 읽음 알림 전송
   */
  sendRead(roomId: number): boolean {
    if (this.socket?.readyState !== WebSocket.OPEN) return false;
    this.send({ type: "READ", room_id: roomId });
    return true;
  }

  /** 사용자가 재시도 버튼 누를 때 수동 재연결 */
  manualReconnect() {
    if (!this.roomId || !this.messageHandler) return;
    this.reconnectAttempts = 0;
    this.connect(
      this.roomId,
      this.messageHandler,
      this.statusHandler ?? undefined,
      this.errorHandler ?? undefined,
    );
  }

  private send(data: object) {
    if (this.socket?.readyState === WebSocket.OPEN) {
      this.socket.send(JSON.stringify(data));
    }
  }

  /** WebSocket 연결 종료 (화면 언마운트 시 호출) */
  disconnect() {
    this.isManualClose = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.socket) {
      this.socket.close();
      this.socket = null;
    }
    this.roomId = null;
    this.messageHandler = null;
    this.statusHandler = null;
    this.errorHandler = null;
    this.reconnectAttempts = 0;
  }

  isConnected(): boolean {
    return this.socket?.readyState === WebSocket.OPEN;
  }

  getStatus(): ConnectionStatus {
    return this.status;
  }
}

// 싱글톤 인스턴스 — 앱 전체에서 하나의 WebSocket 연결만 유지
export const chatSocket = new ChatSocketManager();
