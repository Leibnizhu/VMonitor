package io.github.leibnizhu.vmonitor.wecom.message

/**
 * @author Leibniz on 2020/10/28 2:15 PM
 */
object MessageType extends Enumeration {
  type MessageType = Value
  val Text, Markdown, Image, News, File = Value

  def msgTypeName(msgType: MessageType): String = msgType match {
    case Text => "text"
    case Markdown => "markdown"
    case Image => "image"
    case News => "news"
    case File => "file"
    case _ => throw new IllegalArgumentException("Unsupport MessageType")
  }
}
