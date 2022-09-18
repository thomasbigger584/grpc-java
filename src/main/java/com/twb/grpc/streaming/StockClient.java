package com.twb.grpc.streaming;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.twb.grpc.streaming.StockQuoteProviderGrpc.StockQuoteProviderBlockingStub;
import static com.twb.grpc.streaming.StockQuoteProviderGrpc.StockQuoteProviderStub;
import static com.twb.grpc.streaming.StockQuoteProviderGrpc.newBlockingStub;
import static com.twb.grpc.streaming.StockQuoteProviderGrpc.newStub;

public class StockClient {
    private static final Logger logger =
            LoggerFactory.getLogger(StockClient.class.getName());
    private static final int PORT = 8980;
    private static final String ADDRESS = "localhost";

    private final StockQuoteProviderBlockingStub blockingStub;
    private final StockQuoteProviderStub nonBlockingStub;
    private List<Stock> stocks;

    public StockClient(Channel channel) {

        blockingStub = newBlockingStub(channel);
        nonBlockingStub = newStub(channel);
        initializeStocks();
    }

    public void serverSideStreamingListOfStockPrices() {

        logger.info("######START EXAMPLE######: ServerSideStreaming - list of Stock prices from a given stock");
        Stock request = Stock.newBuilder()
                .setTickerSymbol("AU")
                .setCompanyName("Austich")
                .setDescription("server streaming example")
                .build();
        Iterator<StockQuote> stockQuotes;
        try {
            logger.info("REQUEST - ticker symbol {}", request.getTickerSymbol());
            stockQuotes = blockingStub.serverSideStreamingGetListStockQuotes(request);
            for (int index = 1; stockQuotes.hasNext(); index++) {
                StockQuote stockQuote = stockQuotes.next();
                logger.info("RESPONSE - Price #{}: {}", index, stockQuote.getPrice());
            }
        } catch (StatusRuntimeException e) {
            logger.info("RPC failed: {}", e.getStatus());
        }
    }

    public void clientSideStreamingGetStatisticsOfStocks() throws InterruptedException {

        logger.info("######START EXAMPLE######: ClientSideStreaming - getStatisticsOfStocks from a list of stocks");
        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<StockQuote> responseObserver = new StreamObserver<StockQuote>() {
            @Override
            public void onNext(StockQuote summary) {
                logger.info("RESPONSE, got stock statistics - Average Price: {}, description: {}", summary.getPrice(), summary.getDescription());
            }

            @Override
            public void onCompleted() {
                logger.info("Finished clientSideStreamingGetStatisticsOfStocks");
                finishLatch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("Stock Statistics Failed: {}", Status.fromThrowable(t));
                finishLatch.countDown();
            }
        };

        StreamObserver<Stock> requestObserver = nonBlockingStub.clientSideStreamingGetStatisticsOfStocks(responseObserver);
        try {

            for (Stock stock : stocks) {
                logger.info("REQUEST: {}, {}", stock.getTickerSymbol(), stock.getCompanyName());
                requestObserver.onNext(stock);
                if (finishLatch.getCount() == 0) {
                    return;
                }
            }
        } catch (RuntimeException e) {
            requestObserver.onError(e);
            throw e;
        }
        requestObserver.onCompleted();
        if (!finishLatch.await(1, TimeUnit.MINUTES)) {
            logger.warn("clientSideStreamingGetStatisticsOfStocks can not finish within 1 minutes");
        }
    }

    public void bidirectionalStreamingGetListsStockQuotes() throws InterruptedException {

        logger.info("#######START EXAMPLE#######: BidirectionalStreaming - getListsStockQuotes from list of stocks");
        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<StockQuote> responseObserver = new StreamObserver<StockQuote>() {
            @Override
            public void onNext(StockQuote stockQuote) {
                logger.info("RESPONSE price#{} : {}, description:{}", stockQuote.getOfferNumber(), stockQuote.getPrice(), stockQuote.getDescription());
            }

            @Override
            public void onCompleted() {
                logger.info("Finished bidirectionalStreamingGetListsStockQuotes");
                finishLatch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("bidirectionalStreamingGetListsStockQuotes Failed: {}", Status.fromThrowable(t));
                finishLatch.countDown();
            }
        };
        StreamObserver<Stock> requestObserver = nonBlockingStub.bidirectionalStreamingGetListsStockQuotes(responseObserver);
        try {
            for (Stock stock : stocks) {
                logger.info("REQUEST: {}, {}", stock.getTickerSymbol(), stock.getCompanyName());
                requestObserver.onNext(stock);
                Thread.sleep(200);
                if (finishLatch.getCount() == 0) {
                    return;
                }
            }
        } catch (RuntimeException e) {
            requestObserver.onError(e);
            throw e;
        }
        requestObserver.onCompleted();

        if (!finishLatch.await(1, TimeUnit.MINUTES)) {
            logger.warn("bidirectionalStreamingGetListsStockQuotes can not finish within 1 minute");
        }

    }

    public static void main(String[] args) throws InterruptedException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(ADDRESS, PORT)
                .usePlaintext().build();
        try {
            StockClient client = new StockClient(channel);

            client.serverSideStreamingListOfStockPrices();
            client.clientSideStreamingGetStatisticsOfStocks();
            client.bidirectionalStreamingGetListsStockQuotes();

        } finally {
            channel.shutdownNow()
                    .awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private void initializeStocks() {

        this.stocks = Arrays.asList(Stock.newBuilder().setTickerSymbol("AU").setCompanyName("Auburn Corp").setDescription("Aptitude Intel").build()
                , Stock.newBuilder().setTickerSymbol("BAS").setCompanyName("Bassel Corp").setDescription("Business Intel").build()
                , Stock.newBuilder().setTickerSymbol("COR").setCompanyName("Corvine Corp").setDescription("Corporate Intel").build()
                , Stock.newBuilder().setTickerSymbol("DIA").setCompanyName("Dialogic Corp").setDescription("Development Intel").build()
                , Stock.newBuilder().setTickerSymbol("EUS").setCompanyName("Euskaltel Corp").setDescription("English Intel").build());
    }
}
