package org.example.kafkastreams;

import static org.kafkainaction.ErrorType.INSUFFICIENT_FUNDS;
import static org.kafkainaction.TransactionType.DEPOSIT;

import java.math.BigDecimal;
import java.util.Optional;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.kafkainaction.MyFunds;
import org.kafkainaction.MyTransaction;
import org.kafkainaction.MyTransactionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyTransactionTransformer implements ValueTransformer<MyTransaction, MyTransactionResult> {
    
    private static final Logger log = LoggerFactory.getLogger(MyTransactionTransformer.class);
    
    private final String stateStoreName;
    private KeyValueStore<String, MyFunds> store;
    
    public MyTransactionTransformer() {
        // default name for funds store
        this.stateStoreName = "fundsStore";
    }
    
    public MyTransactionTransformer(final String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }
    
    @Override
    public void close() {
    }
    
    private MyFunds createEmptyFunds(String account) {
        MyFunds funds = new MyFunds(account, BigDecimal.ZERO);
        store.put(account, funds);
        return funds;
    }
    
    private MyFunds depositFunds(MyTransaction transaction) {
        return updateFunds(transaction.getAccount(), transaction.getAmount());
    }
    
    private MyFunds getFunds(String account) {
        return Optional.ofNullable(store.get(account)).orElseGet(() -> createEmptyFunds(account));
    }
    
    private boolean hasEnoughFunds(MyTransaction transaction) {
        return getFunds(transaction.getAccount()).getBalance().compareTo(transaction.getAmount()) != -1;
    }
    
    @Override
    public void init(ProcessorContext context) {
        store = context.getStateStore(stateStoreName);
    }
    
    @Override
    public MyTransactionResult transform(MyTransaction transaction) {
        
        if (transaction.getType().equals(DEPOSIT)) {
            return new MyTransactionResult(transaction, depositFunds(transaction), true, null);
        }
        
        if (hasEnoughFunds(transaction)) {
            return new MyTransactionResult(transaction, withdrawFunds(transaction), true, null);
        }
        
        log.info("Not enough funds for account {}.", transaction.getAccount());
        
        return new MyTransactionResult(transaction, getFunds(transaction.getAccount()), false, INSUFFICIENT_FUNDS);
    }
    
    private MyFunds updateFunds(String account, BigDecimal amount) {
        MyFunds funds = new MyFunds(account, getFunds(account).getBalance().add(amount));
        log.info("Updating funds for account {} with {}. Current balance is {}.", account, amount, funds.getBalance());
        store.put(account, funds);
        return funds;
    }
    
    private MyFunds withdrawFunds(MyTransaction transaction) {
        return updateFunds(transaction.getAccount(), transaction.getAmount().negate());
    }
}
