import {WebSocket} from "uWebSockets.js";
import {Server} from "../server";

export class DataChannels  {
    public channels: Map<string, WebSocket> = new Map();
    public sockets;

    constructor(protected server: Server) {
        this.server = server;
        this.sockets = server.adapter.getNamespace(process.env.PUSHER_APP_ID).sockets;
    }

    async addChannel(ws: WebSocket): Promise<WebSocket> {
            this.channels.set(ws.id, ws);
            return ws;
    }

    async removeChannel(wsId: string): Promise<boolean> {
            if (this.channels.has(wsId)) {
                this.channels.delete(wsId);
            }
            return true;
    }

    async sendChannel(wsId: string, msg: any){
        //let channel = this.channels.get(wsId);
        let channel = this.sockets.get(wsId);
        if (channel === undefined){
            console.log('cannot send to undefined channel    ' + wsId, this.channels);
            return false;
        }
        try {
            channel.sendJson(msg);
            return true;
        } catch (e){
            this.channels.delete(wsId);
            return false;
        }
    }



}
