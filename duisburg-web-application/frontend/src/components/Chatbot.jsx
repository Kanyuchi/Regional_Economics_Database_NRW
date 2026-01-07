import { useState, useRef, useEffect } from 'react';
import './Chatbot.css';

const Chatbot = ({ uiContext = null, chartContext = null }) => {
  const initialMessage = {
    role: 'assistant',
    content: 'Hello! I\'m your Duisburg Economic Dashboard assistant. Ask me anything about the data, trends, or insights from the dashboard.',
    timestamp: new Date(),
  };

  const [isOpen, setIsOpen] = useState(false);
  const [messages, setMessages] = useState([initialMessage]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [isMinimized, setIsMinimized] = useState(false);
  const messagesEndRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  // Toggle a body class so the dashboard can shift when chat is open
  useEffect(() => {
    const className = 'chat-open';
    if (isOpen && !isMinimized) {
      document.body.classList.add(className);
    } else {
      document.body.classList.remove(className);
    }
    return () => document.body.classList.remove(className);
  }, [isOpen, isMinimized]);

  const handleSend = async () => {
    if (!input.trim() || isLoading) return;

    const userMessage = {
      role: 'user',
      content: input.trim(),
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setInput('');
    setIsLoading(true);

    try {
      const response = await fetch('http://localhost:3001/api/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          message: userMessage.content,
          history: messages,
          uiContext,
          chartContext,
        }),
      });

      const data = await response.json();

      const assistantMessage = {
        role: 'assistant',
        content: data.response || 'I apologize, but I encountered an error processing your request.',
        table: data.table || null,
        timestamp: new Date(),
      };

      setMessages((prev) => [...prev, assistantMessage]);
    } catch (error) {
      console.error('Chat error:', error);
      const errorMessage = {
        role: 'assistant',
        content: 'Sorry, I\'m having trouble connecting to the server. Please make sure the backend is running.',
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleReset = () => {
    setMessages([{ ...initialMessage, timestamp: new Date() }]);
    setInput('');
    setIsLoading(false);
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const suggestedQueries = [
    'What\'s the unemployment trend in Duisburg?',
    'Compare GDP across all cities',
    'Show business registration trends',
    'Which city has the highest population?',
  ];

  const handleSuggestedQuery = (query) => {
    setInput(query);
  };

  const renderTable = (table) => {
    if (!table || !table.columns || !table.rows) return null;
    return (
      <div className="chat-table">
        {table.title && <div className="chat-table-title">{table.title}</div>}
        <div className="chat-table-wrapper">
          <table>
            <thead>
              <tr>
                {table.columns.map((col) => (
                  <th key={col}>{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {table.rows.map((row, idx) => (
                <tr key={idx}>
                  {row.map((cell, cidx) => (
                    <td key={`${idx}-${cidx}`}>{cell}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  return (
    <>
      {/* Floating Chat Button */}
      <button
        className={`chat-button ${isOpen ? 'open' : ''}`}
        onClick={() => setIsOpen(!isOpen)}
        aria-label="Toggle chat"
      >
        {isOpen ? (
          <svg
            width="24"
            height="24"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
          >
            <line x1="18" y1="6" x2="6" y2="18" />
            <line x1="6" y1="6" x2="18" y2="18" />
          </svg>
        ) : (
          <svg
            width="24"
            height="24"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
          >
            <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
          </svg>
        )}
      </button>

      {/* Chat Panel */}
      {isOpen && (
        <div className={`chat-panel ${isMinimized ? 'minimized' : ''}`}>
          <div className="chat-header">
            <div className="chat-header-info">
              <h3>AI Assistant</h3>
              <span className="chat-status">
                <span className="status-dot"></span>
                Online
              </span>
            </div>
            <div className="chat-header-actions">
              <button
                className="chat-reset"
                onClick={() => setIsMinimized(!isMinimized)}
                aria-label={isMinimized ? 'Expand chat' : 'Minimize chat'}
              >
                {isMinimized ? 'Expand' : 'Minimize'}
              </button>
              <button
                className="chat-reset"
                onClick={handleReset}
                aria-label="Start new chat"
              >
                New chat
              </button>
              <button
                className="chat-close"
                onClick={() => setIsOpen(false)}
                aria-label="Close chat"
              >
                ✕
              </button>
            </div>
          </div>

          {!isMinimized && (
            <>
              <div className="chat-messages">
                {messages.map((msg, idx) => (
                  <div key={idx} className={`message ${msg.role}`}>
                    <div className="message-avatar">
                      {msg.role === 'assistant' ? '🤖' : '👤'}
                    </div>
                    <div className="message-content">
                      <div className="message-text">{msg.content}</div>
                      {msg.table && renderTable(msg.table)}
                      <div className="message-time">
                        {msg.timestamp.toLocaleTimeString('en-US', {
                          hour: '2-digit',
                          minute: '2-digit',
                        })}
                      </div>
                    </div>
                  </div>
                ))}

                {isLoading && (
                  <div className="message assistant">
                    <div className="message-avatar">🤖</div>
                    <div className="message-content">
                      <div className="typing-indicator">
                        <span></span>
                        <span></span>
                        <span></span>
                      </div>
                    </div>
                  </div>
                )}

                <div ref={messagesEndRef} />
              </div>

              {messages.length === 1 && (
                <div className="suggested-queries">
                  <p className="suggested-label">Try asking:</p>
                  {suggestedQueries.map((query, idx) => (
                    <button
                      key={idx}
                      className="suggested-query"
                      onClick={() => handleSuggestedQuery(query)}
                    >
                      {query}
                    </button>
                  ))}
                </div>
              )}

              <div className="chat-input-container">
                <textarea
                  className="chat-input"
                  placeholder="Ask about the data..."
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  onKeyPress={handleKeyPress}
                  rows={1}
                  disabled={isLoading}
                />
                <button
                  className="chat-send"
                  onClick={handleSend}
                  disabled={!input.trim() || isLoading}
                  aria-label="Send message"
                >
                  <svg
                    width="20"
                    height="20"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                  >
                    <line x1="22" y1="2" x2="11" y2="13" />
                    <polygon points="22 2 15 22 11 13 2 9 22 2" />
                  </svg>
                </button>
              </div>
            </>
          )}
        </div>
      )}
    </>
  );
};

export default Chatbot;
