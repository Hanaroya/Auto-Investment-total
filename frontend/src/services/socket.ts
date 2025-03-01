import { io, Socket } from 'socket.io-client';
import { WebSocketMessage } from '@/types/socket';

class SocketService {
  private static instance: SocketService;
  private socket: Socket | null = null;
  private messageHandlers: Map<string, ((data: any) => void)[]> = new Map();

  private constructor() {
    // 싱글톤 패턴
  }

  public static getInstance(): SocketService {
    if (!SocketService.instance) {
      SocketService.instance = new SocketService();
    }
    return SocketService.instance;
  }

  public connect(url: string = 'ws://localhost:8000'): void {
    if (this.socket) {
      console.warn('Socket is already connected');
      return;
    }

    this.socket = io(url, {
      reconnection: true,
      reconnectionAttempts: 5,
      reconnectionDelay: 1000,
    });

    this.setupEventListeners();
  }

  private setupEventListeners(): void {
    if (!this.socket) return;

    this.socket.on('connect', () => {
      console.log('Connected to WebSocket server');
    });

    this.socket.on('disconnect', () => {
      console.log('Disconnected from WebSocket server');
    });

    this.socket.on('error', (error) => {
      console.error('WebSocket error:', error);
    });

    this.socket.on('message', (message: WebSocketMessage) => {
      const handlers = this.messageHandlers.get(message.type) || [];
      handlers.forEach(handler => handler(message.data));
    });
  }

  public subscribe<T>(type: string, handler: (data: T) => void): () => void {
    const handlers = this.messageHandlers.get(type) || [];
    handlers.push(handler);
    this.messageHandlers.set(type, handlers);

    // 구독 취소 함수 반환
    return () => {
      const handlers = this.messageHandlers.get(type) || [];
      const index = handlers.indexOf(handler);
      if (index > -1) {
        handlers.splice(index, 1);
        this.messageHandlers.set(type, handlers);
      }
    };
  }

  public disconnect(): void {
    if (this.socket) {
      this.socket.disconnect();
      this.socket = null;
      this.messageHandlers.clear();
    }
  }

  public isConnected(): boolean {
    return this.socket?.connected || false;
  }

  public emit(event: string, data: any): void {
    if (this.socket && this.socket.connected) {
      this.socket.emit(event, data);
    } else {
      console.warn('Socket is not connected');
    }
  }
}

export const socketService = SocketService.getInstance(); 