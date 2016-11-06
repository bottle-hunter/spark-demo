package org.soltys.spark.demo.service;

import java.io.Serializable;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Interface which contains functionality to split text into words.
 *
 * @author Mykhailo Soltys
 */
public interface WordCounter extends Serializable {

    /**
     * Splits input string into words
     *
     * @param text input String
     * @return words in a {@link Iterator<String>} representation
     */
    static Iterator<String> splitWords(String text) {
        List<String> words = new ArrayList<>();
        BreakIterator breakIterator = BreakIterator.getWordInstance();
        breakIterator.setText(text);
        int lastIndex = breakIterator.first();
        while (BreakIterator.DONE != lastIndex) {
            int firstIndex = lastIndex;
            lastIndex = breakIterator.next();
            if (lastIndex != BreakIterator.DONE && Character.isLetterOrDigit(text.charAt(firstIndex))) {
                words.add(text.substring(firstIndex, lastIndex));
            }
        }
        return words.iterator();
    }
}
