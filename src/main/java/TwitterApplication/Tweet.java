package TwitterApplication;

import java.util.List;

/**
 * Created by shailendralohia on 7/9/18.
 */
public class Tweet {

    private String language;
    private String text;
    private List<String> tags;

    @Override
    public String toString() {
        return "Tweet{" +
                "language='" + language + '\'' +
                ", text='" + text + '\'' +
                ", tags=" + tags +
                '}';
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Tweet(String language, String text, List<String> tags) {
        this.language = language;
        this.text = text;
        this.tags = tags;
    }




}
