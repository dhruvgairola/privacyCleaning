package data.cleaning.core.utils.kdtree;

public class CosineDistance implements DistanceMetric {

	@Override
	public float distance(float[] a, float[] b) {
		float num = 0f;
		float acomp = 0f;
		float bcomp = 0f;

		for (int i = 0; i < a.length; i++) {
			num += a[i] * b[i];
			acomp += a[i] * a[i];
			bcomp += b[i] * b[i];
		}

		float denom = (float) (Math.sqrt(acomp) * Math.sqrt(bcomp));

		float similarity = num / denom;

		float angular = (float) (1d - ((2 * Math.acos(similarity)) / Math.PI));

		return angular;
	}

}
