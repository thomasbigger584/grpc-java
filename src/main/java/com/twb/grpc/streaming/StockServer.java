package com.twb.grpc.streaming;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class StockServer {
    private static final int SERVER_PORT = 8980;
    private static final Logger logger =
            LoggerFactory.getLogger(StockServer.class.getName());
    private final Server server;

    public StockServer() {
        server = ServerBuilder.forPort(SERVER_PORT)
                .addService(new StockService())
                .build();
    }

    public void start() throws IOException {
        server.start();
        logger.info("Server started, listening on {}", SERVER_PORT);

        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> {
                    logger.info("Shutting down server");
                    try {
                        StockServer.this.stop();
                    } catch (InterruptedException e) {
                        logger.error("StockServer stopping has been interrupted", e);
                    }
                    logger.info("Server shut down complete");
                }));
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown()
                    .awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    public static void main(String[] args) throws Exception {
        StockServer stockServer = new StockServer();
        stockServer.start();
        if (stockServer.server != null) {
            stockServer.server.awaitTermination();
        }
        logger.info("Stock server started on port {}", SERVER_PORT);
    }

    private static class StockService extends StockQuoteProviderGrpc.StockQuoteProviderImplBase {

        StockService() {
        }

        @Override
        public void serverSideStreamingGetListStockQuotes(Stock request, StreamObserver<StockQuote> responseObserver) {

            for (int index = 1; index <= 5; index++) {

                StockQuote stockQuote = StockQuote.newBuilder()
                        .setPrice(fetchStockPriceBid(request))
                        .setOfferNumber(index)
                        .setDescription("Price for stock:" + request.getTickerSymbol())
                        .build();
                responseObserver.onNext(stockQuote);
            }
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<Stock> clientSideStreamingGetStatisticsOfStocks(final StreamObserver<StockQuote> responseObserver) {
            return new StreamObserver<Stock>() {
                int count;
                double price = 0.0;
                final StringBuilder sb = new StringBuilder();

                @Override
                public void onNext(Stock stock) {
                    count++;
                    price = +fetchStockPriceBid(stock);
                    sb.append(":")
                            .append(stock.getTickerSymbol());
                }

                @Override
                public void onCompleted() {
                    responseObserver.onNext(StockQuote.newBuilder()
                            .setPrice(price / count)
                            .setDescription("Statistics-" + sb)
                            .build());
                    responseObserver.onCompleted();
                }

                @Override
                public void onError(Throwable t) {
                    logger.warn("error:{}", t.getMessage());
                }
            };
        }

        @Override
        public StreamObserver<Stock> bidirectionalStreamingGetListsStockQuotes(final StreamObserver<StockQuote> responseObserver) {
            return new StreamObserver<Stock>() {
                @Override
                public void onNext(Stock request) {

                    for (int index = 1; index <= 5; index++) {

                        StockQuote stockQuote = StockQuote.newBuilder()
                                .setPrice(fetchStockPriceBid(request))
                                .setOfferNumber(index)
                                .setDescription("Price for stock:" + request.getTickerSymbol())
                                .build();
                        responseObserver.onNext(stockQuote);
                    }
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }

                @Override
                public void onError(Throwable t) {
                    logger.error("error:{}", t.getMessage());
                }
            };
        }
    }

    private static double fetchStockPriceBid(Stock stock) {

        return stock.getTickerSymbol()
                .length()
                + ThreadLocalRandom.current()
                .nextDouble(-0.1d, 0.1d);
    }
}