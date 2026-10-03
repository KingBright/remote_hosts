package io.remotehosts.agent
import android.app.Activity
import android.os.Bundle
import android.text.InputType
import android.widget.*
/** Only packaged in the separate debug application. */
class ProbeActivity: Activity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val root = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; setPadding(30, 100, 30, 30) }
        val label = TextView(this).apply { text = "Remote Hosts integration probe"; textSize = 24f }
        val input = EditText(this).apply { setText("hello"); contentDescription = "fixture-input" }
        val password = EditText(this).apply { inputType = InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_VARIATION_PASSWORD; setText("fixture-sensitive-value") }
        val button = Button(this).apply { text = "Apply fixture text"; setOnClickListener { label.text = "已确认：" + input.text } }
        root.addView(label); root.addView(input); root.addView(password); root.addView(button); setContentView(root)
    }
}
