package com.demo.transactional;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.Session;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.OptimisticLockType;
import org.hibernate.annotations.OptimisticLocking;
import org.hibernate.stat.SessionStatistics;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.orm.jpa.EntityManagerHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.persistence.*;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

interface PersonRepository extends JpaRepository<Person, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Person findById(Person person);
}

@SpringBootApplication
public class DemoTransactionalApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoTransactionalApplication.class, args);
    }
}

@Service
@RequiredArgsConstructor
class Initializer implements CommandLineRunner {

    private final PersonRepository personRepository;

    private final AnotherClass anotherClass;

    @Override
    @Transactional // before and after
    public void run(String... args) {
//        IntStream.range(0, 3).parallel().forEach(value -> anotherClass.showTrx());
//        boolean trxStatus = TransactionSynchronizationManager.isActualTransactionActive();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        personRepository.save(Person.valueOf("dummy"));
        
        System.out.println("============== INSERT PERSON ============== " + TransactionSynchronizationManager.getCurrentTransactionName() + " ==== WITH ENTITY KEYS ==== " + anotherClass.getEntityKeys());

//        try {
//            anotherClass.saveAnotherPerson();
//        } catch (Exception e) {
//            System.out.println("=============== ERROR CURR TRX IS ROLLBACK: " + TransactionAspectSupport.currentTransactionStatus().isRollbackOnly() + " ============================");
//            System.out.println("=============== ERROR CURR TRX CONTEXT: " + anotherClass.getEntityKeys() + " ============================");
//            anotherClass.updateError(person);
//        }

//        throw new RuntimeException("Error aja dah");

//        CompletableFuture.runAsync(anotherClass::saveAnotherPerson)
//            .exceptionally(e -> {
//                anotherClass.updateError(person);
//                return null;
//            });

//        executorService.shutdown();
    }
}

@Service
@RequiredArgsConstructor
class AnotherClass {

    private final PersonRepository personRepository;

    @Transactional(rollbackFor = RuntimeException.class) // -> AOP
    @SneakyThrows
    public void saveAnotherPerson() {
        personRepository.save(Person.valueOf("another"));
        System.out.println("============== INSERT PERSON ERROR ============== " + TransactionSynchronizationManager.getCurrentTransactionName() + " ==== WITH ENTITY KEYS ==== " + getEntityKeys());

        throw new RuntimeException("ERROR");
    }

    public void showTrx() {
        System.out.println(TransactionSynchronizationManager.isActualTransactionActive() + " " + Thread.currentThread());
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void updateError(Person person) {
//        personRepository.findById(id)
//            .map(person -> {
//                person.setName("ERROR NICH");
//                return person;
//            })
//            .orElse(null);

        person.setName("ERROR NICH");
        personRepository.save(Person.valueOf("LALA"));

        System.out.println("============== UPDATE ERROR PERSON ============== " + TransactionSynchronizationManager.getCurrentTransactionName() + " ==== WITH ENTITY KEYS ==== " + getEntityKeys());
    }

    public Set<?> getEntityKeys() {
        return TransactionSynchronizationManager.getResourceMap()
            .values()
            .stream()
            .filter(EntityManagerHolder.class::isInstance)
            .map(EntityManagerHolder.class::cast)
            .map(EntityManagerHolder::getEntityManager)
            .map(em -> em.unwrap(Session.class))
            .map(Session::getStatistics)
            .map(SessionStatistics::getEntityKeys)
            .findFirst()
            .orElse(null);
    }
}

@Entity
@Table(name = "persons")
@Data
@NoArgsConstructor
@EqualsAndHashCode(of = "id")
@AllArgsConstructor
@ToString
@OptimisticLocking(type = OptimisticLockType.ALL)
@DynamicUpdate
class Person {

    @Id
    @GeneratedValue
    private Long id;

    @Column
    private String name;

    private Person(String name) {
        this.name = name;
    }

    public static Person valueOf(String name) {
        return new Person(name);
    }
}

@RestController
@RequiredArgsConstructor
class DummyController {

    private final TransactionalService transactionalService;

    @GetMapping(path = "/dummy")
    public void testTransactions() {
        transactionalService.test();
    }
}

@Service
@RequiredArgsConstructor
@Slf4j
class TransactionalService {

    private final PersonRepository personRepository;

    private final ExecutorService executor = Executors.newWorkStealingPool();

    @Transactional
    public void test() {
        boolean isTx = TransactionSynchronizationManager.isActualTransactionActive();
        String name = TransactionSynchronizationManager.getCurrentTransactionName();
        log.info("Thread {} transactional is active: {} with name {}", Thread.currentThread().getName(), isTx, name);

        Runnable runnable = () -> personRepository.findAll().parallelStream().forEach(this::log);
        CompletableFuture.runAsync(runnable, executor);
    }

    private void log(Person person) {
        boolean isTx = TransactionSynchronizationManager.isActualTransactionActive();
        String name = TransactionSynchronizationManager.getCurrentTransactionName();
        log.info(String.format(
            "Person [%s] => Thread %s transactional is active: %s with name %s",
            person,
            Thread.currentThread().getName(),
            isTx,
            name)
        );
    }

    @Transactional(isolation = Isolation.SERIALIZABLE)
    public void update() {
        List<Person> persons = personRepository.findAll(); // 3 => pessimistic locking
        // do something

        // do something again

        persons = personRepository.findAll(); // 3
    }
    // lock released

    @Transactional
    public void update2() {
        Person person = personRepository.save(new Person()); // 2.

        // blocking

        System.out.println(person); //Ganteng
    }
}




// Trx => set of operations which act as a whole
// Atomic           => yes or no
// Consistency      => adhere to constraints
// Isolation        => encapsulate the operation
// 1. READ UNCOMMITTED 2. READ COMMITTED 3. REPEATABLE READ 4. SERIALIZABLE
// Durability       => durable.

// 1. READ UNCOMMITTED => NO GUARD => DIRTY READ
//    BEFORE ROW 1 NAME : GANTENG
//    TRX A -> ROW 1 NAME : DIMAS => EXCEPTION => ROLLBACK GANTENG
//    TRX B -> ROW 1 : DIMAS => ANOMALY

// 2. READ COMMITTED => COMMIT ALREADY PREVENT DIRTY READ
//    BEFORE ROW 1 NAME : GANTENG
//    TRX A -> ROW 1 NAME : DIMAS => EXCEPTION => ROLLBACK GANTENG
//    TRX B -> ROW 1 : GANTENG => ANOMALY

// 3. REPEATABLE READ => PREVENT NON REPEATABLE READ
//    BEFORE ROW 1 KOSONG
//    TRX A -> SELECT ROW 1 => EMPTY
//          -> SELECT ROW 1 => FOUND
//    TRX B -> INSERT ROW 1.

//    TRX A -> SELECT ROW 1 => EMPTY
//          -> SELECT ROW 1 => EMPTY
//    TRX B -> INSERT ROW 1. => DONE PERSIST

// 3. SERIALIZABLE => PREVENT PHANTOM READ
//    BEFORE SELECT WHERE STATE = 'INACTIVE'
//    TRX A -> SELECT  => 3 ROW
//          -> SELECT  => 4 ROW
//    TRX B -> INSERT 1 ROW .

//    TRX A -> SELECT => 3 ROW
//          -> SELECT => 3 ROW
//          -> FINISH
//    TRX B -> INSERT ROW 1. => WAITING TRX A FINISHED
//          -> INSERTED

// DATABASE LOCKING => DATABASE CONCURRENCY CONTROL


// DIMAS AMBIL DUIT 1M DI ATM BMW
// ISTRI DIMAS AMBIL DUIT 1M DI ATM MERCEDES