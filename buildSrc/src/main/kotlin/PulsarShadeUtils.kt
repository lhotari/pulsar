import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

/** Relocate a package under the shade prefix: `"x.y"` → `"$prefix.x.y"` */
fun ShadowJar.relocateWithPrefix(prefix: String, pattern: String) {
    relocate(pattern, "$prefix.$pattern")
}

/** Fix ahc-default.properties to use the relocated asynchttpclient package name. */
fun ShadowJar.relocateAsyncHttpClientProperties(prefix: String) {
    filesMatching("org/asynchttpclient/config/ahc-default.properties") {
        filter { line -> line.replace("org.asynchttpclient.", "$prefix.org.asynchttpclient.") }
    }
}
