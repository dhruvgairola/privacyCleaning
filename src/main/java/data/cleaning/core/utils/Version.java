package data.cleaning.core.utils;

public enum Version {
	// BASIC_REMOVE_EXACT_MATCHES is a lot less memory intensive and much
	// cheaper cf REMOVE_EXACT_MATCHES but not as granular.
	MEMORY_SAVER, REMOVE_EXACT_MATCHES, BASIC_REMOVE_EXACT_MATCHES, REMOVE_DUPS_FROM_MATCHES, NORMAL
}
