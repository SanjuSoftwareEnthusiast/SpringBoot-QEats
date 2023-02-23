package com.crio.warmup.stock.portfolio;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.crio.warmup.stock.dto.AnnualizedReturn;
import com.crio.warmup.stock.dto.Candle;
import com.crio.warmup.stock.dto.PortfolioTrade;
import com.crio.warmup.stock.exception.StockQuoteServiceException;
import com.crio.warmup.stock.quotes.StockQuotesService;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.springframework.web.client.RestTemplate;

public class PortfolioManagerImpl implements PortfolioManager {


  private StockQuotesService stockQuotesService;

  private RestTemplate restTemplate;


  // Caution: Do not delete or modify the constructor, or else your build will break!
  // This is absolutely necessary for backward compatibility
  protected PortfolioManagerImpl(RestTemplate restTemplate) {
    this.restTemplate = restTemplate;
  }

  PortfolioManagerImpl(StockQuotesService stockQuotesService) {
    this.stockQuotesService = stockQuotesService;
  }


  // TODO: CRIO_TASK_MODULE_REFACTOR
  // 1. Now we want to convert our code into a module, so we will not call it from main anymore.
  // Copy your code from Module#3 PortfolioManagerApplication#calculateAnnualizedReturn
  // into #calculateAnnualizedReturn function here and ensure it follows the method signature.
  // 2. Logic to read Json file and convert them into Objects will not be required further as our
  // clients will take care of it, going forward.

  // Note:
  // Make sure to exercise the tests inside PortfolioManagerTest using command below:
  // ./gradlew test --tests PortfolioManagerTest

  // CHECKSTYLE:OFF



  private Comparator<AnnualizedReturn> getComparator() {
    return Comparator.comparing(AnnualizedReturn::getAnnualizedReturn).reversed();
  }

  // CHECKSTYLE:OFF

  // TODO: CRIO_TASK_MODULE_REFACTOR
  // Extract the logic to call Tiingo third-party APIs to a separate function.
  // Remember to fill out the buildUri function and use that.


  public List<Candle> getStockQuote(String symbol, LocalDate from, LocalDate to)
      throws JsonProcessingException, StockQuoteServiceException {
    return stockQuotesService.getStockQuote(symbol, from, to);
  }

  @Override
  public List<AnnualizedReturn> calculateAnnualizedReturn(List<PortfolioTrade> portfolioTrades,
      LocalDate endDate) throws StockQuoteServiceException {
    AnnualizedReturn annualizedReturn;
    List<AnnualizedReturn> annualizedReturns = new ArrayList<AnnualizedReturn>();

    for (int i = 0; i < portfolioTrades.size(); i++) {
      annualizedReturn = getAnnualizedReturn(portfolioTrades.get(i), endDate);

      annualizedReturns.add(annualizedReturn);
    }

    Comparator<AnnualizedReturn> sortByAnnReturn =
        Comparator.comparing(AnnualizedReturn::getAnnualizedReturn).reversed();

    Collections.sort(annualizedReturns, sortByAnnReturn);

    return annualizedReturns;
  }

  public AnnualizedReturn getAnnualizedReturn(PortfolioTrade trade, LocalDate endLocalDate)
      throws StockQuoteServiceException {
    AnnualizedReturn annualizedReturn;
    String symbol = trade.getSymbol();
    LocalDate startLocalDate = trade.getPurchaseDate();

    try {

      List<Candle> stocksStartToEndDate;

      stocksStartToEndDate = getStockQuote(symbol, startLocalDate, endLocalDate);

      Candle stocksStartDate = stocksStartToEndDate.get(0);
      Candle stocksEndDate = stocksStartToEndDate.get(stocksStartToEndDate.size() - 1);

      Double buyPrice = stocksStartDate.getOpen();
      Double sellPrice = stocksEndDate.getClose();

      Double totalReturn = (sellPrice - buyPrice) / buyPrice;

      Double numYears = (double) ChronoUnit.DAYS.between(startLocalDate, endLocalDate) / 365;

      Double annualizedReturns = Math.pow((1 + totalReturn), (1 / numYears)) - 1;

      annualizedReturn = new AnnualizedReturn(symbol, annualizedReturns, totalReturn);

    } catch (JsonProcessingException e) {
      return new AnnualizedReturn(symbol, Double.NaN, Double.NaN);
    }

    return annualizedReturn;
  }

  @Override
  public List<AnnualizedReturn> calculateAnnualizedReturnParallel(
      List<PortfolioTrade> portfolioTrades, LocalDate endDate, int numThreads)
      throws InterruptedException, StockQuoteServiceException {
    List<AnnualizedReturn> annualizedReturns = new ArrayList<AnnualizedReturn>();

    List<Future<AnnualizedReturn>> futureReturnsList = new ArrayList<Future<AnnualizedReturn>>();

    long startTime = System.currentTimeMillis();
    final ExecutorService pool = Executors.newFixedThreadPool(numThreads);

    for (int i = 0; i < portfolioTrades.size(); i++) {
      PortfolioTrade trade = portfolioTrades.get(i);

      Callable<AnnualizedReturn> callableTask = () -> {
        return getAnnualizedReturn(trade, endDate);
      };

      Future<AnnualizedReturn> futureReturns = pool.submit(callableTask);

      futureReturnsList.add(futureReturns);
    }

    for (int i = 0; i < portfolioTrades.size(); i++) {
      Future<AnnualizedReturn> futureReturns = futureReturnsList.get(i);
      try{
        AnnualizedReturn returns = futureReturns.get();
        annualizedReturns.add(returns);
      }catch(ExecutionException e){
        throw new StockQuoteServiceException("Error while calling API",e);
      }
    }

    annualizedReturns.sort(new Comparator<AnnualizedReturn>() {
      @Override
      public int compare(AnnualizedReturn p1, AnnualizedReturn p2) {
          return (int)(p1.getAnnualizedReturn().compareTo(p2.getAnnualizedReturn()));
      }
  });
    // Collections.sort(annualizedReturns, Collections.reverseOrder());
    Collections.reverse(annualizedReturns);
    System.out.println(annualizedReturns.get(0).getAnnualizedReturn());
    System.out.println(annualizedReturns.get(1).getAnnualizedReturn());
    System.out.println(annualizedReturns.get(2).getAnnualizedReturn());
    
    System.out.println(System.currentTimeMillis() - startTime );

    return annualizedReturns;
  }
}
