package cn._51doit.day03;

import java.util.Objects;

/**
 * 定义封装数据的POJO（java bean）
 */
public class WordCount {

    private String word;

    private Integer count;

    public WordCount() {};

    public WordCount(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WordCount wordCount = (WordCount) o;
        return Objects.equals(word, wordCount.word);
    }

    @Override
    public int hashCode() {
        return word.hashCode();
    }
}