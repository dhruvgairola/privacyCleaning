package data.cleaning.core.misc;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Any miscellaneous tests/hacks/experiments can be added here.
 * 
 * @author dhruvgairola
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class MiscTests {
	private static final Logger logger = Logger.getLogger(MiscTests.class);
	private Set<Foo> FOO = EnumSet.of(Foo.BAR, Foo.BARRE);

	@Test
	public void testArrKeyVal() throws Exception {
		Map<float[], String> m = new HashMap<>();
		float[] k = new float[] { 0.2f, 0.8f };
		float[] k2 = new float[] { 0.2f, 0.8f };
		m.put(k, "foo");
		Assert.assertTrue(!m.containsKey(k2));
	}

	@Test
	public void testEnumSet() throws Exception {
		Assert.assertTrue(FOO.containsAll(EnumSet.of(Foo.BAR, Foo.BARRE)));
	}

	public enum Foo {
		BAR, BARRE, BARRED
	}

	public class Bar {
		private String b;

		public String getB() {
			return b;
		}

		public void setB(String b) {
			this.b = b;
		}

		@Override
		public String toString() {
			return b;
		}

	}
}
