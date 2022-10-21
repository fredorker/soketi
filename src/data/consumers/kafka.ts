import {Log} from "../../log";
import { Server } from '../../server';
import {DataChannels} from "../data-channels";

const { Kafka } = require('kafkajs')

const { KAFKA_USERNAME: username, KAFKA_PASSWORD: password } = process.env
const sasl = username && password ? { username, password, mechanism: 'plain' } : null;
const ssl = !!sasl
const websockets_topic = 'WS_' + process.env.TAG;
const subscriptions_topic = 'SUBSCRIPTIONS_' + process.env.TAG;
const client_id = 'websockets_' + process.env.TAG;

export class DataConsumer {

    private consumer;
    private producer;
    private dataChannels;

    constructor(protected server: Server) {
        this.server = server;
        const kafka = new Kafka({
            clientId: client_id,
            brokers: [process.env.KAFKA_BOOTSTRAP_SERVER_A + ':' + process.env.KAFKA_BOOTSTRAP_SERVER_A_PORT],
            ssl,
            sasl
        });
        this.dataChannels = new DataChannels(this.server);
        let kafkaOptions = {
            groupId: client_id,
            minBytes: 5,
            maxBytes: 1e6,
            maxWaitTimeInMs: 100,
        };
        this.consumer = kafka.consumer(kafkaOptions);
        this.producer = kafka.producer();

        this.consume().catch(async error => {
            console.error(error)
            try {
                await this.consumer.disconnect()
            } catch (e) {
                console.error('Failed to gracefully disconnect consumer', e)
            }
            process.exit(1)
        })
    }

    public consume = async () => {
        await this.producer.connect();

        // Let the post office know that all sockets and thus all data channels are gone.
        this.producer.send({
            topic: subscriptions_topic,
            messages: [
                {value: JSON.stringify({"cmd":"remove_all_channels"})},
            ],
            acks: 1
        })
        await this.consumer.connect()
        await this.consumer.subscribe({
            topic: websockets_topic,
            fromBeginning: false
        });

        await this.consumer.run({
            eachMessage: async ({topic, partition, message}) => {
                let data = JSON.parse(message.value.toString());
                this.dataChannels.sendChannel(data.channel, data.u).then( success => {
                    if (!success){
                        console.log('invalid channel, removing ...', data.channel);
                        this.producer.send({
                            topic: subscriptions_topic,
                            messages: [
                                {value: JSON.stringify( {"cmd":"remove_channel", "channel": data.channel})},
                            ],
                        })
                    }
                });
            }
        })
    }
}
