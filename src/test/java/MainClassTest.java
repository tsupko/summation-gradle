import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Alexander Tsupko (tsupko.alexander@yandex.ru)
 *         Copyright (c) 2016. All rights reserved.
 */
public class MainClassTest {
    @BeforeClass
    public static void setUp() {
        MainClass.main();
    }

    @Test
    public void test() {
        assertEquals(4101227820028L, MainClass.getTotal());
    }
}